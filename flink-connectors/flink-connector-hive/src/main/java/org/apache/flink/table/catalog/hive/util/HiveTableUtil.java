/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.hive.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalogConfig;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileStorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.ALTER_TABLE_OP;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.SERDE_INFO_PROP_PREFIX;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.SERDE_LIB_CLASS_NAME;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_FILE_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_INPUT_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_OUTPUT_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.TABLE_IS_EXTERNAL;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.TABLE_LOCATION_URI;
import static org.apache.flink.table.catalog.CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Utils to for Hive-backed table. */
public class HiveTableUtil {

    private static final byte HIVE_CONSTRAINT_ENABLE = 1 << 2;
    private static final byte HIVE_CONSTRAINT_VALIDATE = 1 << 1;
    private static final byte HIVE_CONSTRAINT_RELY = 1;

    private static final StorageFormatFactory storageFormatFactory = new StorageFormatFactory();

    private HiveTableUtil() {}

    /** Create a Flink's TableSchema from Hive table's columns and partition keys. */
    public static TableSchema createTableSchema(
            List<FieldSchema> cols,
            List<FieldSchema> partitionKeys,
            Set<String> notNullColumns,
            UniqueConstraint primaryKey) {
        List<FieldSchema> allCols = new ArrayList<>(cols);
        allCols.addAll(partitionKeys);

        String[] colNames = new String[allCols.size()];
        DataType[] colTypes = new DataType[allCols.size()];

        for (int i = 0; i < allCols.size(); i++) {
            FieldSchema fs = allCols.get(i);

            colNames[i] = fs.getName();
            colTypes[i] =
                    HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
            if (notNullColumns.contains(colNames[i])) {
                colTypes[i] = colTypes[i].notNull();
            }
        }

        TableSchema.Builder builder = TableSchema.builder().fields(colNames, colTypes);
        if (primaryKey != null) {
            builder.primaryKey(
                    primaryKey.getName(), primaryKey.getColumns().toArray(new String[0]));
        }
        return builder.build();
    }

    /** Create Hive columns from Flink TableSchema. */
    public static List<FieldSchema> createHiveColumns(TableSchema schema) {
        String[] fieldNames = schema.getFieldNames();
        DataType[] fieldTypes = schema.getFieldDataTypes();

        List<FieldSchema> columns = new ArrayList<>(fieldNames.length);

        for (int i = 0; i < fieldNames.length; i++) {
            columns.add(
                    new FieldSchema(
                            fieldNames[i],
                            HiveTypeUtil.toHiveTypeInfo(fieldTypes[i], true).getTypeName(),
                            null));
        }

        return columns;
    }

    // --------------------------------------------------------------------------------------------
    //  Helper methods
    // --------------------------------------------------------------------------------------------

    /** Creates a Hive partition instance. */
    public static Partition createHivePartition(
            String dbName,
            String tableName,
            List<String> values,
            StorageDescriptor sd,
            Map<String, String> parameters) {
        Partition partition = new Partition();
        partition.setDbName(dbName);
        partition.setTableName(tableName);
        partition.setValues(values);
        partition.setParameters(parameters);
        partition.setSd(sd);
        int currentTime = (int) (System.currentTimeMillis() / 1000);
        partition.setCreateTime(currentTime);
        partition.setLastAccessTime(currentTime);
        return partition;
    }

    // returns a constraint trait that requires ENABLE
    public static byte enableConstraint(byte trait) {
        return (byte) (trait | HIVE_CONSTRAINT_ENABLE);
    }

    // returns a constraint trait that requires VALIDATE
    public static byte validateConstraint(byte trait) {
        return (byte) (trait | HIVE_CONSTRAINT_VALIDATE);
    }

    // returns a constraint trait that requires RELY
    public static byte relyConstraint(byte trait) {
        return (byte) (trait | HIVE_CONSTRAINT_RELY);
    }

    // returns whether a trait requires ENABLE constraint
    public static boolean requireEnableConstraint(byte trait) {
        return (trait & HIVE_CONSTRAINT_ENABLE) != 0;
    }

    // returns whether a trait requires VALIDATE constraint
    public static boolean requireValidateConstraint(byte trait) {
        return (trait & HIVE_CONSTRAINT_VALIDATE) != 0;
    }

    // returns whether a trait requires RELY constraint
    public static boolean requireRelyConstraint(byte trait) {
        return (trait & HIVE_CONSTRAINT_RELY) != 0;
    }

    /**
     * Generates a filter string for partition columns from the given filter expressions.
     *
     * @param partColOffset The number of non-partition columns -- used to shift field reference
     *     index
     * @param partColNames The names of all partition columns
     * @param expressions The filter expressions in CNF form
     * @return an Optional filter string equivalent to the expressions, which is empty if the
     *     expressions can't be handled
     */
    public static Optional<String> makePartitionFilter(
            int partColOffset,
            List<String> partColNames,
            List<Expression> expressions,
            HiveShim hiveShim) {
        List<String> filters = new ArrayList<>(expressions.size());
        ExpressionExtractor extractor =
                new ExpressionExtractor(partColOffset, partColNames, hiveShim);
        for (Expression expression : expressions) {
            String str = expression.accept(extractor);
            if (str == null) {
                return Optional.empty();
            }
            filters.add(str);
        }
        return Optional.of(String.join(" and ", filters));
    }

    /**
     * Extract DDL semantics from properties and use it to initiate the table. The related
     * properties will be removed from the map after they're used.
     */
    public static void initiateTableFromProperties(
            Table hiveTable, Map<String, String> properties, HiveConf hiveConf) {
        extractExternal(hiveTable, properties);
        extractRowFormat(hiveTable.getSd(), properties);
        extractStoredAs(hiveTable.getSd(), properties, hiveConf);
        extractLocation(hiveTable.getSd(), properties);
    }

    private static void extractExternal(Table hiveTable, Map<String, String> properties) {
        boolean external = Boolean.parseBoolean(properties.remove(TABLE_IS_EXTERNAL));
        if (external) {
            hiveTable.setTableType(TableType.EXTERNAL_TABLE.toString());
            // follow Hive to set this property
            hiveTable.getParameters().put("EXTERNAL", "TRUE");
        }
    }

    public static void extractLocation(StorageDescriptor sd, Map<String, String> properties) {
        String location = properties.remove(TABLE_LOCATION_URI);
        if (location != null) {
            sd.setLocation(location);
        }
    }

    public static void extractRowFormat(StorageDescriptor sd, Map<String, String> properties) {
        String serdeLib = properties.remove(SERDE_LIB_CLASS_NAME);
        if (serdeLib != null) {
            sd.getSerdeInfo().setSerializationLib(serdeLib);
        }
        List<String> serdeProps =
                properties.keySet().stream()
                        .filter(p -> p.startsWith(SERDE_INFO_PROP_PREFIX))
                        .collect(Collectors.toList());
        for (String prop : serdeProps) {
            String value = properties.remove(prop);
            // there was a typo of this property in hive, and was fixed in 3.0.0 --
            // https://issues.apache.org/jira/browse/HIVE-16922
            String key =
                    prop.equals(HiveTableRowFormat.COLLECTION_DELIM)
                            ? serdeConstants.COLLECTION_DELIM
                            : prop.substring(SERDE_INFO_PROP_PREFIX.length());
            sd.getSerdeInfo().getParameters().put(key, value);
            // make sure FIELD_DELIM and SERIALIZATION_FORMAT are consistent
            if (key.equals(serdeConstants.FIELD_DELIM)) {
                sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, value);
            }
        }
    }

    private static void extractStoredAs(
            StorageDescriptor sd, Map<String, String> properties, HiveConf hiveConf) {
        String storageFormat = properties.remove(STORED_AS_FILE_FORMAT);
        String inputFormat = properties.remove(STORED_AS_INPUT_FORMAT);
        String outputFormat = properties.remove(STORED_AS_OUTPUT_FORMAT);
        if (storageFormat == null && inputFormat == null) {
            return;
        }
        if (storageFormat != null) {
            setStorageFormat(sd, storageFormat, hiveConf);
        } else {
            sd.setInputFormat(inputFormat);
            sd.setOutputFormat(outputFormat);
        }
    }

    public static void setStorageFormat(StorageDescriptor sd, String format, HiveConf hiveConf) {
        StorageFormatDescriptor storageFormatDescriptor = storageFormatFactory.get(format);
        checkArgument(storageFormatDescriptor != null, "Unknown storage format " + format);
        sd.setInputFormat(storageFormatDescriptor.getInputFormat());
        sd.setOutputFormat(storageFormatDescriptor.getOutputFormat());
        String serdeLib = storageFormatDescriptor.getSerde();
        if (serdeLib == null && storageFormatDescriptor instanceof RCFileStorageFormatDescriptor) {
            serdeLib = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
        }
        if (serdeLib != null) {
            sd.getSerdeInfo().setSerializationLib(serdeLib);
        }
    }

    public static void setDefaultStorageFormat(StorageDescriptor sd, HiveConf hiveConf) {
        sd.getSerdeInfo().setSerializationLib(hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTSERDE));
        setStorageFormat(sd, hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT), hiveConf);
    }

    public static void alterColumns(StorageDescriptor sd, CatalogTable catalogTable) {
        List<FieldSchema> allCols = HiveTableUtil.createHiveColumns(catalogTable.getSchema());
        List<FieldSchema> nonPartCols =
                allCols.subList(0, allCols.size() - catalogTable.getPartitionKeys().size());
        sd.setCols(nonPartCols);
    }

    public static SqlAlterHiveTable.AlterTableOp extractAlterTableOp(Map<String, String> props) {
        String opStr = props.remove(ALTER_TABLE_OP);
        if (opStr != null) {
            return SqlAlterHiveTable.AlterTableOp.valueOf(opStr);
        }
        return null;
    }

    public static Table alterTableViaCatalogBaseTable(
            ObjectPath tablePath,
            CatalogBaseTable baseTable,
            Table oldHiveTable,
            HiveConf hiveConf) {
        Table newHiveTable = instantiateHiveTable(tablePath, baseTable, hiveConf);
        // client.alter_table() requires a valid location
        // thus, if new table doesn't have that, it reuses location of the old table
        if (!newHiveTable.getSd().isSetLocation()) {
            newHiveTable.getSd().setLocation(oldHiveTable.getSd().getLocation());
        }
        return newHiveTable;
    }

    public static Table instantiateHiveTable(
            ObjectPath tablePath, CatalogBaseTable table, HiveConf hiveConf) {
        // let Hive set default parameters for us, e.g. serialization.format
        Table hiveTable =
                org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(
                        tablePath.getDatabaseName(), tablePath.getObjectName());
        hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));

        Map<String, String> properties = new HashMap<>(table.getOptions());
        // Table comment
        if (table.getComment() != null) {
            properties.put(HiveCatalogConfig.COMMENT, table.getComment());
        }

        boolean isGeneric = HiveCatalog.isGenericForCreate(properties);

        // Hive table's StorageDescriptor
        StorageDescriptor sd = hiveTable.getSd();
        HiveTableUtil.setDefaultStorageFormat(sd, hiveConf);

        if (isGeneric) {
            DescriptorProperties tableSchemaProps = new DescriptorProperties(true);
            tableSchemaProps.putTableSchema(Schema.SCHEMA, table.getSchema());

            if (table instanceof CatalogTable) {
                tableSchemaProps.putPartitionKeys(((CatalogTable) table).getPartitionKeys());
            }

            properties.putAll(tableSchemaProps.asMap());
            properties = maskFlinkProperties(properties);
            hiveTable.setParameters(properties);
        } else {
            HiveTableUtil.initiateTableFromProperties(hiveTable, properties, hiveConf);
            List<FieldSchema> allColumns = HiveTableUtil.createHiveColumns(table.getSchema());
            // Table columns and partition keys
            if (table instanceof CatalogTable) {
                CatalogTable catalogTable = (CatalogTable) table;

                if (catalogTable.isPartitioned()) {
                    int partitionKeySize = catalogTable.getPartitionKeys().size();
                    List<FieldSchema> regularColumns =
                            allColumns.subList(0, allColumns.size() - partitionKeySize);
                    List<FieldSchema> partitionColumns =
                            allColumns.subList(
                                    allColumns.size() - partitionKeySize, allColumns.size());

                    sd.setCols(regularColumns);
                    hiveTable.setPartitionKeys(partitionColumns);
                } else {
                    sd.setCols(allColumns);
                    hiveTable.setPartitionKeys(new ArrayList<>());
                }
            } else {
                sd.setCols(allColumns);
            }
            // Table properties
            hiveTable.getParameters().putAll(properties);
        }

        if (table instanceof CatalogView) {
            // TODO: [FLINK-12398] Support partitioned view in catalog API
            hiveTable.setPartitionKeys(new ArrayList<>());

            CatalogView view = (CatalogView) table;
            hiveTable.setViewOriginalText(view.getOriginalQuery());
            hiveTable.setViewExpandedText(view.getExpandedQuery());
            hiveTable.setTableType(TableType.VIRTUAL_VIEW.name());
        }

        return hiveTable;
    }

    /**
     * Add a prefix to Flink-created properties to distinguish them from Hive-created properties.
     * Note that 'is_generic' is a special key and this method will leave it as-is.
     */
    public static Map<String, String> maskFlinkProperties(Map<String, String> properties) {
        return properties.entrySet().stream()
                .filter(e -> e.getKey() != null && e.getValue() != null)
                .map(
                        e ->
                                new Tuple2<>(
                                        e.getKey().equals(CatalogPropertiesUtil.IS_GENERIC)
                                                ? e.getKey()
                                                : FLINK_PROPERTY_PREFIX + e.getKey(),
                                        e.getValue()))
                .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
    }

    /**
     * Check whether to read or write on the hive ACID table.
     *
     * @param catalogTable Hive catalog table.
     * @param tablePath Identifier table path.
     * @throws FlinkHiveException Thrown, if the source or sink table is transactional.
     */
    public static void checkAcidTable(CatalogTable catalogTable, ObjectPath tablePath) {
        String tableIsTransactional = catalogTable.getOptions().get("transactional");
        if (tableIsTransactional == null) {
            tableIsTransactional = catalogTable.getOptions().get("transactional".toUpperCase());
        }
        if (tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true")) {
            throw new FlinkHiveException(
                    String.format("Reading or writing ACID table %s is not supported.", tablePath));
        }
    }

    /**
     * Returns a new Hadoop Configuration object using the path to the hadoop conf configured.
     *
     * @param hadoopConfDir Hadoop conf directory path.
     * @return A Hadoop configuration instance.
     */
    public static Configuration getHadoopConfiguration(String hadoopConfDir) {
        if (new File(hadoopConfDir).exists()) {
            Configuration hadoopConfiguration = new Configuration();
            File coreSite = new File(hadoopConfDir, "core-site.xml");
            if (coreSite.exists()) {
                hadoopConfiguration.addResource(new Path(coreSite.getAbsolutePath()));
            }
            File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
            if (hdfsSite.exists()) {
                hadoopConfiguration.addResource(new Path(hdfsSite.getAbsolutePath()));
            }
            File yarnSite = new File(hadoopConfDir, "yarn-site.xml");
            if (yarnSite.exists()) {
                hadoopConfiguration.addResource(new Path(yarnSite.getAbsolutePath()));
            }
            // Add mapred-site.xml. We need to read configurations like compression codec.
            File mapredSite = new File(hadoopConfDir, "mapred-site.xml");
            if (mapredSite.exists()) {
                hadoopConfiguration.addResource(new Path(mapredSite.getAbsolutePath()));
            }
            return hadoopConfiguration;
        }
        return null;
    }

    private static class ExpressionExtractor implements ExpressionVisitor<String> {

        // maps a supported function to its name
        private static final Map<FunctionDefinition, String> FUNC_TO_STR = new HashMap<>();

        static {
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.EQUALS, "=");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.NOT_EQUALS, "<>");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.GREATER_THAN, ">");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, ">=");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.LESS_THAN, "<");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, "<=");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.AND, "and");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.OR, "or");
        }

        // used to shift field reference index
        private final int partColOffset;
        private final List<String> partColNames;
        private final HiveShim hiveShim;

        ExpressionExtractor(int partColOffset, List<String> partColNames, HiveShim hiveShim) {
            this.partColOffset = partColOffset;
            this.partColNames = partColNames;
            this.hiveShim = hiveShim;
        }

        @Override
        public String visit(CallExpression call) {
            FunctionDefinition funcDef = call.getFunctionDefinition();
            if (FUNC_TO_STR.containsKey(funcDef)) {
                List<String> operands = new ArrayList<>();
                for (Expression child : call.getChildren()) {
                    String operand = child.accept(this);
                    if (operand == null) {
                        return null;
                    }
                    operands.add(operand);
                }
                return "(" + String.join(" " + FUNC_TO_STR.get(funcDef) + " ", operands) + ")";
            }
            return null;
        }

        @Override
        public String visit(ValueLiteralExpression valueLiteral) {
            DataType dataType = valueLiteral.getOutputDataType();
            Object value = valueLiteral.getValueAs(Object.class).orElse(null);
            if (value == null) {
                return "null";
            }
            LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
            if (typeRoot.getFamilies().contains(LogicalTypeFamily.DATETIME)) {
                // hive not support partition filter push down with these types.
                return null;
            }
            value =
                    HiveInspectors.getConversion(
                                    HiveInspectors.getObjectInspector(dataType),
                                    dataType.getLogicalType(),
                                    hiveShim)
                            .toHiveObject(value);
            String res = value.toString();
            if (typeRoot == LogicalTypeRoot.CHAR || typeRoot == LogicalTypeRoot.VARCHAR) {
                res = "'" + res.replace("'", "''") + "'";
            }
            return res;
        }

        @Override
        public String visit(FieldReferenceExpression fieldReference) {
            return partColNames.get(fieldReference.getFieldIndex() - partColOffset);
        }

        @Override
        public String visit(TypeLiteralExpression typeLiteral) {
            return typeLiteral.getOutputDataType().toString();
        }

        @Override
        public String visit(Expression other) {
            // only support resolved expressions
            return null;
        }
    }
}
