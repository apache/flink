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

package org.apache.flink.table.planner.delegation.hive.parse;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.sql.parser.hive.ddl.HiveDDLUtils;
import org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.CatalogViewImpl;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.factories.HiveFunctionDefinitionFactory;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.DescribeTableOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ShowDatabasesOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowPartitionsOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.ShowViewsOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.ddl.AddPartitionsOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterPartitionPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableOptionsOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.AlterTableSchemaOperation;
import org.apache.flink.table.operations.ddl.AlterViewAsOperation;
import org.apache.flink.table.operations.ddl.AlterViewPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterViewRenameOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTableASOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.operations.ddl.DropPartitionsOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropViewOperation;
import org.apache.flink.table.planner.delegation.hive.HiveBucketSpec;
import org.apache.flink.table.planner.delegation.hive.HiveParser;
import org.apache.flink.table.planner.delegation.hive.HiveParserCalcitePlanner;
import org.apache.flink.table.planner.delegation.hive.HiveParserConstants;
import org.apache.flink.table.planner.delegation.hive.HiveParserDMLHelper;
import org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTNode;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserAuthorizationParseUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserContext;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQueryState;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserStorageFormat;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.antlr.runtime.tree.CommonTree;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.sql.parser.hive.ddl.HiveDDLUtils.COL_DELIMITER;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase.ALTER_DATABASE_OP;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabaseOwner.DATABASE_OWNER_NAME;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabaseOwner.DATABASE_OWNER_TYPE;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.ALTER_COL_CASCADE;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.ALTER_TABLE_OP;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.ALTER_BUCKET;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.ALTER_COLUMNS;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.CHANGE_FILE_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.CHANGE_LOCATION;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.CHANGE_SERDE_PROPS;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.CHANGE_TBL_PROPS;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveDatabase.DATABASE_LOCATION_URI;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.COLLECTION_DELIM;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.ESCAPE_CHAR;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.FIELD_DELIM;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.LINE_DELIM;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.MAPKEY_DELIM;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.SERDE_INFO_PROP_PREFIX;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.SERDE_LIB_CLASS_NAME;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.SERIALIZATION_NULL_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_FILE_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_INPUT_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_OUTPUT_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.NOT_NULL_COLS;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.NOT_NULL_CONSTRAINT_TRAITS;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.PK_CONSTRAINT_TRAIT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.TABLE_IS_EXTERNAL;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.TABLE_LOCATION_URI;
import static org.apache.flink.table.planner.delegation.hive.HiveBucketSpec.dumpBucketNumToProp;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.NotNullConstraint;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.PrimaryKey;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getBucketSpec;

/**
 * Ported hive's org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer, and also incorporated
 * functionalities from SemanticAnalyzer and FunctionSemanticAnalyzer. It's mainly used to convert
 * {@link HiveParserASTNode} to the corresponding {@link Operation}.
 */
public class HiveParserDDLSemanticAnalyzer {
    private static final Logger LOG = LoggerFactory.getLogger(HiveParserDDLSemanticAnalyzer.class);
    private static final Map<Integer, String> TokenToTypeName = new HashMap<>();

    private final Set<String> reservedPartitionValues;
    private final HiveConf conf;
    private final HiveParserQueryState queryState;
    private final HiveCatalog hiveCatalog;
    private final CatalogManager catalogManager;
    private final String currentDB;
    private final HiveParser hiveParser;
    private final HiveFunctionDefinitionFactory funcDefFactory;
    private final HiveShim hiveShim;
    private final HiveParserContext context;
    private final HiveParserDMLHelper dmlHelper;

    static {
        TokenToTypeName.put(HiveASTParser.TOK_BOOLEAN, serdeConstants.BOOLEAN_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_TINYINT, serdeConstants.TINYINT_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_SMALLINT, serdeConstants.SMALLINT_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_INT, serdeConstants.INT_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_BIGINT, serdeConstants.BIGINT_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_FLOAT, serdeConstants.FLOAT_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_DOUBLE, serdeConstants.DOUBLE_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_STRING, serdeConstants.STRING_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_CHAR, serdeConstants.CHAR_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_VARCHAR, serdeConstants.VARCHAR_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_BINARY, serdeConstants.BINARY_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_DATE, serdeConstants.DATE_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_DATETIME, serdeConstants.DATETIME_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME);
        TokenToTypeName.put(
                HiveASTParser.TOK_INTERVAL_YEAR_MONTH,
                HiveParserConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
        TokenToTypeName.put(
                HiveASTParser.TOK_INTERVAL_DAY_TIME,
                HiveParserConstants.INTERVAL_DAY_TIME_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_DECIMAL, serdeConstants.DECIMAL_TYPE_NAME);
    }

    public static String getTypeName(HiveParserASTNode node) throws SemanticException {
        int token = node.getType();
        String typeName;

        // datetime type isn't currently supported
        if (token == HiveASTParser.TOK_DATETIME) {
            throw new ValidationException(ErrorMsg.UNSUPPORTED_TYPE.getMsg());
        }

        switch (token) {
            case HiveASTParser.TOK_CHAR:
                CharTypeInfo charTypeInfo = HiveASTParseUtils.getCharTypeInfo(node);
                typeName = charTypeInfo.getQualifiedName();
                break;
            case HiveASTParser.TOK_VARCHAR:
                VarcharTypeInfo varcharTypeInfo = HiveASTParseUtils.getVarcharTypeInfo(node);
                typeName = varcharTypeInfo.getQualifiedName();
                break;
            case HiveASTParser.TOK_DECIMAL:
                DecimalTypeInfo decTypeInfo = HiveASTParseUtils.getDecimalTypeTypeInfo(node);
                typeName = decTypeInfo.getQualifiedName();
                break;
            default:
                typeName = TokenToTypeName.get(token);
        }
        return typeName;
    }

    public HiveParserDDLSemanticAnalyzer(
            HiveParserQueryState queryState,
            HiveCatalog hiveCatalog,
            CatalogManager catalogManager,
            HiveParser hiveParser,
            HiveShim hiveShim,
            HiveParserContext context,
            HiveParserDMLHelper dmlHelper)
            throws SemanticException {
        this.queryState = queryState;
        this.conf = queryState.getConf();
        this.hiveCatalog = hiveCatalog;
        this.currentDB = catalogManager.getCurrentDatabase();
        this.catalogManager = catalogManager;
        this.hiveParser = hiveParser;
        this.funcDefFactory = new HiveFunctionDefinitionFactory(hiveShim);
        this.hiveShim = hiveShim;
        this.context = context;
        this.dmlHelper = dmlHelper;
        reservedPartitionValues = new HashSet<>();
        // Partition can't have this name
        reservedPartitionValues.add(HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME));
        reservedPartitionValues.add(
                HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULT_ZOOKEEPER_PARTITION_NAME));
        // Partition value can't end in this suffix
        reservedPartitionValues.add(
                HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_INT_ORIGINAL));
        reservedPartitionValues.add(
                HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_INT_ARCHIVED));
        reservedPartitionValues.add(
                HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_INT_EXTRACTED));
    }

    private Table getTable(ObjectPath tablePath) {
        try {
            return new Table(hiveCatalog.getHiveTable(tablePath));
        } catch (TableNotExistException e) {
            throw new ValidationException("Table not found", e);
        }
    }

    public Operation convertToOperation(HiveParserASTNode ast) throws SemanticException {
        Operation res = null;
        switch (ast.getType()) {
            case HiveASTParser.TOK_ALTERTABLE:
                res = convertAlterTable(ast);
                break;
            case HiveASTParser.TOK_DROPTABLE:
                res = convertDropTable(ast, null);
                break;
            case HiveASTParser.TOK_DESCTABLE:
                res = convertDescribeTable(ast);
                break;
            case HiveASTParser.TOK_SHOWDATABASES:
                res = convertShowDatabases();
                break;
            case HiveASTParser.TOK_SHOWTABLES:
                res = convertShowTables(ast, false);
                break;
            case HiveASTParser.TOK_SHOWFUNCTIONS:
                res = convertShowFunctions(ast);
                break;
            case HiveASTParser.TOK_SHOWVIEWS:
                res = convertShowTables(ast, true);
                break;
            case HiveASTParser.TOK_DROPVIEW:
                res = convertDropTable(ast, TableType.VIRTUAL_VIEW);
                break;
            case HiveASTParser.TOK_ALTERVIEW:
                res = convertAlterView(ast);
                break;
            case HiveASTParser.TOK_SHOWPARTITIONS:
                res = convertShowPartitions(ast);
                break;
            case HiveASTParser.TOK_CREATEDATABASE:
                res = convertCreateDatabase(ast);
                break;
            case HiveASTParser.TOK_DROPDATABASE:
                res = convertDropDatabase(ast);
                break;
            case HiveASTParser.TOK_SWITCHDATABASE:
                res = convertSwitchDatabase(ast);
                break;
            case HiveASTParser.TOK_ALTERDATABASE_PROPERTIES:
                res = convertAlterDatabaseProperties(ast);
                break;
            case HiveASTParser.TOK_ALTERDATABASE_OWNER:
                res = convertAlterDatabaseOwner(ast);
                break;
            case HiveASTParser.TOK_ALTERDATABASE_LOCATION:
                res = convertAlterDatabaseLocation(ast);
                break;
            case HiveASTParser.TOK_CREATETABLE:
                res = convertCreateTable(ast);
                break;
            case HiveASTParser.TOK_CREATEVIEW:
                res = convertCreateView(ast);
                break;
            case HiveASTParser.TOK_CREATEFUNCTION:
                res = convertCreateFunction(ast);
                break;
            case HiveASTParser.TOK_DROPFUNCTION:
                res = convertDropFunction(ast);
                break;
            case HiveASTParser.TOK_DESCFUNCTION:
            case HiveASTParser.TOK_DESCDATABASE:
            case HiveASTParser.TOK_TRUNCATETABLE:
            case HiveASTParser.TOK_CREATEINDEX:
            case HiveASTParser.TOK_DROPINDEX:
            case HiveASTParser.TOK_SHOWLOCKS:
            case HiveASTParser.TOK_SHOWDBLOCKS:
            case HiveASTParser.TOK_SHOW_COMPACTIONS:
            case HiveASTParser.TOK_SHOW_TRANSACTIONS:
            case HiveASTParser.TOK_ABORT_TRANSACTIONS:
            case HiveASTParser.TOK_MSCK:
            case HiveASTParser.TOK_ALTERINDEX_REBUILD:
            case HiveASTParser.TOK_ALTERINDEX_PROPERTIES:
            case HiveASTParser.TOK_SHOWINDEXES:
            case HiveASTParser.TOK_LOCKTABLE:
            case HiveASTParser.TOK_UNLOCKTABLE:
            case HiveASTParser.TOK_LOCKDB:
            case HiveASTParser.TOK_UNLOCKDB:
            case HiveASTParser.TOK_CREATEROLE:
            case HiveASTParser.TOK_DROPROLE:
            case HiveASTParser.TOK_SHOW_ROLE_GRANT:
            case HiveASTParser.TOK_SHOW_ROLE_PRINCIPALS:
            case HiveASTParser.TOK_SHOW_ROLES:
            case HiveASTParser.TOK_GRANT_ROLE:
            case HiveASTParser.TOK_REVOKE_ROLE:
            case HiveASTParser.TOK_GRANT:
            case HiveASTParser.TOK_SHOW_GRANT:
            case HiveASTParser.TOK_REVOKE:
            case HiveASTParser.TOK_SHOW_SET_ROLE:
            case HiveASTParser.TOK_CACHE_METADATA:
            case HiveASTParser.TOK_DROP_MATERIALIZED_VIEW:
            case HiveASTParser.TOK_SHOW_CREATEDATABASE:
            case HiveASTParser.TOK_SHOWCOLUMNS:
            case HiveASTParser.TOK_SHOW_TABLESTATUS:
            case HiveASTParser.TOK_SHOW_TBLPROPERTIES:
            case HiveASTParser.TOK_SHOWCONF:
            case HiveASTParser.TOK_SHOW_CREATETABLE:
            default:
                handleUnsupportedOperation(ast);
        }
        return res;
    }

    private Operation convertAlterTable(HiveParserASTNode input) throws SemanticException {
        Operation operation = null;
        HiveParserASTNode ast = (HiveParserASTNode) input.getChild(1);
        String[] qualified =
                HiveParserBaseSemanticAnalyzer.getQualifiedTableName(
                        (HiveParserASTNode) input.getChild(0));
        String tableName = HiveParserBaseSemanticAnalyzer.getDotName(qualified);
        HashMap<String, String> partSpec = null;
        HiveParserASTNode partSpecNode = (HiveParserASTNode) input.getChild(2);
        if (partSpecNode != null) {
            partSpec = getPartSpec(partSpecNode);
        }
        CatalogBaseTable alteredTable = getAlteredTable(tableName, false);
        switch (ast.getType()) {
            case HiveASTParser.TOK_ALTERTABLE_RENAME:
                operation = convertAlterTableRename(tableName, ast, false);
                break;
            case HiveASTParser.TOK_ALTERTABLE_ADDCOLS:
                operation = convertAlterTableModifyCols(alteredTable, tableName, ast, false);
                break;
            case HiveASTParser.TOK_ALTERTABLE_REPLACECOLS:
                operation = convertAlterTableModifyCols(alteredTable, tableName, ast, true);
                break;
            case HiveASTParser.TOK_ALTERTABLE_RENAMECOL:
                operation = convertAlterTableChangeCol(alteredTable, qualified, ast);
                break;
            case HiveASTParser.TOK_ALTERTABLE_ADDPARTS:
                operation = convertAlterTableAddParts(qualified, ast);
                break;
            case HiveASTParser.TOK_ALTERTABLE_DROPPARTS:
                operation = convertAlterTableDropParts(qualified, ast);
                break;
            case HiveASTParser.TOK_ALTERTABLE_PROPERTIES:
                operation =
                        convertAlterTableProps(alteredTable, tableName, null, ast, false, false);
                break;
            case HiveASTParser.TOK_ALTERTABLE_DROPPROPERTIES:
                operation = convertAlterTableProps(alteredTable, tableName, null, ast, false, true);
                break;
            case HiveASTParser.TOK_ALTERTABLE_UPDATESTATS:
                operation =
                        convertAlterTableProps(
                                alteredTable, tableName, partSpec, ast, false, false);
                break;
            case HiveASTParser.TOK_ALTERTABLE_FILEFORMAT:
                operation = convertAlterTableFileFormat(alteredTable, ast, tableName, partSpec);
                break;
            case HiveASTParser.TOK_ALTERTABLE_LOCATION:
                operation = convertAlterTableLocation(alteredTable, ast, tableName, partSpec);
                break;
            case HiveASTParser.TOK_ALTERTABLE_SERIALIZER:
                operation = convertAlterTableSerde(alteredTable, ast, tableName, partSpec);
                break;
            case HiveASTParser.TOK_ALTERTABLE_SERDEPROPERTIES:
                operation = convertAlterTableSerdeProps(alteredTable, ast, tableName, partSpec);
                break;
            case HiveASTParser.TOK_ALTERTABLE_BUCKETS:
                operation = convertAlterTableBucketNum(alteredTable, ast, tableName, partSpec);
                break;
            case HiveASTParser.TOK_ALTERTABLE_CLUSTER_SORT:
                operation = convertAlterTableClusterSort(alteredTable, ast, tableName, partSpec);
                break;
            case HiveASTParser.TOK_ALTERTABLE_TOUCH:
            case HiveASTParser.TOK_ALTERTABLE_ARCHIVE:
            case HiveASTParser.TOK_ALTERTABLE_UNARCHIVE:
            case HiveASTParser.TOK_ALTERTABLE_PARTCOLTYPE:
            case HiveASTParser.TOK_ALTERTABLE_SKEWED:
            case HiveASTParser.TOK_ALTERTABLE_EXCHANGEPARTITION:
            case HiveASTParser.TOK_ALTERTABLE_MERGEFILES:
            case HiveASTParser.TOK_ALTERTABLE_RENAMEPART:
            case HiveASTParser.TOK_ALTERTABLE_SKEWED_LOCATION:
            case HiveASTParser.TOK_ALTERTABLE_COMPACT:
            case HiveASTParser.TOK_ALTERTABLE_UPDATECOLSTATS:
            case HiveASTParser.TOK_ALTERTABLE_DROPCONSTRAINT:
            case HiveASTParser.TOK_ALTERTABLE_ADDCONSTRAINT:
                handleUnsupportedOperation(ast);
                break;
            default:
                throw new ValidationException("Unknown AST node for ALTER TABLE: " + ast);
        }
        return operation;
    }

    private Operation convertDropFunction(HiveParserASTNode ast) {
        // ^(TOK_DROPFUNCTION identifier ifExists? $temp?)
        String functionName = ast.getChild(0).getText();
        boolean ifExists = (ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS) != null);

        boolean isTemporaryFunction =
                (ast.getFirstChildWithType(HiveASTParser.TOK_TEMPORARY) != null);
        if (isTemporaryFunction) {
            return new DropTempSystemFunctionOperation(functionName, ifExists);
        } else {
            ObjectIdentifier identifier = parseObjectIdentifier(functionName);
            return new DropCatalogFunctionOperation(identifier, ifExists, false);
        }
    }

    private Operation convertCreateFunction(HiveParserASTNode ast) {
        // ^(TOK_CREATEFUNCTION identifier StringLiteral ({isTempFunction}? => TOK_TEMPORARY))
        String functionName = ast.getChild(0).getText().toLowerCase();
        boolean isTemporaryFunction =
                (ast.getFirstChildWithType(HiveASTParser.TOK_TEMPORARY) != null);
        String className =
                HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(1).getText());

        // Temp functions are not allowed to have qualified names.
        if (isTemporaryFunction && FunctionUtils.isQualifiedFunctionName(functionName)) {
            // hive's temporary function is more like flink's temp system function, e.g. doesn't
            // belong to a catalog/db
            throw new ValidationException(
                    "Temporary function cannot be created with a qualified name.");
        }

        if (isTemporaryFunction) {
            FunctionDefinition funcDefinition =
                    funcDefFactory.createFunctionDefinition(
                            functionName,
                            new CatalogFunctionImpl(className, FunctionLanguage.JAVA));
            return new CreateTempSystemFunctionOperation(functionName, false, funcDefinition);
        } else {
            ObjectIdentifier identifier = parseObjectIdentifier(functionName);
            CatalogFunction catalogFunction =
                    new CatalogFunctionImpl(className, FunctionLanguage.JAVA);
            return new CreateCatalogFunctionOperation(identifier, catalogFunction, false, false);
        }
    }

    private Operation convertAlterView(HiveParserASTNode ast) throws SemanticException {
        Operation operation = null;
        String[] qualified =
                HiveParserBaseSemanticAnalyzer.getQualifiedTableName(
                        (HiveParserASTNode) ast.getChild(0));
        String tableName = HiveParserBaseSemanticAnalyzer.getDotName(qualified);
        CatalogBaseTable alteredTable = getAlteredTable(tableName, true);
        if (ast.getChild(1).getType() == HiveASTParser.TOK_QUERY) {
            // alter view as
            operation = convertCreateView(ast);
        } else {
            ast = (HiveParserASTNode) ast.getChild(1);
            switch (ast.getType()) {
                case HiveASTParser.TOK_ALTERVIEW_PROPERTIES:
                    operation =
                            convertAlterTableProps(alteredTable, tableName, null, ast, true, false);
                    break;
                case HiveASTParser.TOK_ALTERVIEW_DROPPROPERTIES:
                    operation =
                            convertAlterTableProps(alteredTable, tableName, null, ast, true, true);
                    break;
                case HiveASTParser.TOK_ALTERVIEW_RENAME:
                    operation = convertAlterTableRename(tableName, ast, true);
                    break;
                case HiveASTParser.TOK_ALTERVIEW_ADDPARTS:
                case HiveASTParser.TOK_ALTERVIEW_DROPPARTS:
                    handleUnsupportedOperation("ADD/DROP PARTITION for view is not supported");
                    break;
                default:
                    throw new ValidationException("Unknown AST node for ALTER VIEW: " + ast);
            }
        }
        return operation;
    }

    private Operation convertCreateView(HiveParserASTNode ast) throws SemanticException {
        String[] qualTabName =
                HiveParserBaseSemanticAnalyzer.getQualifiedTableName(
                        (HiveParserASTNode) ast.getChild(0));
        String dbDotTable = HiveParserBaseSemanticAnalyzer.getDotName(qualTabName);
        List<FieldSchema> cols = null;
        boolean ifNotExists = false;
        boolean isAlterViewAs = false;
        String comment = null;
        HiveParserASTNode selectStmt = null;
        Map<String, String> tblProps = null;
        boolean isMaterialized =
                ast.getToken().getType() == HiveASTParser.TOK_CREATE_MATERIALIZED_VIEW;
        if (isMaterialized) {
            handleUnsupportedOperation("MATERIALIZED VIEW is not supported");
        }
        HiveParserStorageFormat storageFormat = new HiveParserStorageFormat(conf);

        LOG.info("Creating view " + dbDotTable + " position=" + ast.getCharPositionInLine());
        int numCh = ast.getChildCount();
        for (int num = 1; num < numCh; num++) {
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(num);
            if (storageFormat.fillStorageFormat(child)) {
                handleUnsupportedOperation("FILE FORMAT for view is not supported");
            }
            switch (child.getToken().getType()) {
                case HiveASTParser.TOK_IFNOTEXISTS:
                    ifNotExists = true;
                    break;
                case HiveASTParser.TOK_REWRITE_ENABLED:
                    handleUnsupportedOperation("MATERIALIZED VIEW REWRITE is not supported");
                    break;
                case HiveASTParser.TOK_ORREPLACE:
                    handleUnsupportedOperation("CREATE OR REPLACE VIEW is not supported");
                    break;
                case HiveASTParser.TOK_QUERY:
                    selectStmt = child;
                    break;
                case HiveASTParser.TOK_TABCOLNAME:
                    cols = HiveParserBaseSemanticAnalyzer.getColumns(child);
                    break;
                case HiveASTParser.TOK_TABLECOMMENT:
                    comment =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(0).getText());
                    break;
                case HiveASTParser.TOK_TABLEPROPERTIES:
                    tblProps = getProps((HiveParserASTNode) child.getChild(0));
                    break;
                case HiveASTParser.TOK_TABLEROWFORMAT:
                    handleUnsupportedOperation("ROW FORMAT for view is not supported");
                    break;
                case HiveASTParser.TOK_TABLESERIALIZER:
                    handleUnsupportedOperation("SERDE for view is not supported");
                    break;
                case HiveASTParser.TOK_TABLELOCATION:
                    handleUnsupportedOperation("LOCATION for view is not supported");
                    break;
                case HiveASTParser.TOK_VIEWPARTCOLS:
                    handleUnsupportedOperation("PARTITION COLUMN for view is not supported");
                    break;
                default:
                    throw new ValidationException(
                            "Unknown AST node for CREATE/ALTER VIEW: " + child);
            }
        }

        if (ast.getToken().getType() == HiveASTParser.TOK_ALTERVIEW
                && ast.getChild(1).getType() == HiveASTParser.TOK_QUERY) {
            isAlterViewAs = true;
        }

        queryState.setCommandType(HiveOperation.CREATEVIEW);

        HiveParserCreateViewInfo createViewInfo =
                new HiveParserCreateViewInfo(dbDotTable, cols, selectStmt);
        hiveParser.analyzeCreateView(createViewInfo, context, queryState, hiveShim);

        ObjectIdentifier viewIdentifier = parseObjectIdentifier(createViewInfo.getCompoundName());
        TableSchema schema =
                HiveTableUtil.createTableSchema(
                        createViewInfo.getSchema(),
                        Collections.emptyList(),
                        Collections.emptySet(),
                        null);
        Map<String, String> props = new HashMap<>();
        if (isAlterViewAs) {
            CatalogBaseTable baseTable = getCatalogBaseTable(viewIdentifier);
            props.putAll(baseTable.getOptions());
            comment = baseTable.getComment();
        } else {
            if (tblProps != null) {
                props.putAll(tblProps);
            }
        }
        CatalogView catalogView =
                new CatalogViewImpl(
                        createViewInfo.getOriginalText(),
                        createViewInfo.getExpandedText(),
                        schema,
                        props,
                        comment);
        if (isAlterViewAs) {
            return new AlterViewAsOperation(viewIdentifier, catalogView);
        } else {
            return new CreateViewOperation(viewIdentifier, catalogView, ifNotExists, false);
        }
    }

    private Operation convertCreateTable(HiveParserASTNode ast) throws SemanticException {
        String[] qualifiedTabName =
                HiveParserBaseSemanticAnalyzer.getQualifiedTableName(
                        (HiveParserASTNode) ast.getChild(0));
        String dbDotTab = HiveParserBaseSemanticAnalyzer.getDotName(qualifiedTabName);

        String likeTableName;
        List<FieldSchema> cols = new ArrayList<>();
        List<FieldSchema> partCols = new ArrayList<>();
        List<PrimaryKey> primaryKeys = new ArrayList<>();
        List<NotNullConstraint> notNulls = new ArrayList<>();
        String comment = null;
        String location = null;
        Map<String, String> tblProps = null;
        boolean ifNotExists = false;
        boolean isExt = false;
        boolean isTemporary = false;
        HiveParserASTNode selectStmt = null;
        final int createTable = 0; // regular CREATE TABLE
        final int ctlt = 1; // CREATE TABLE LIKE ... (CTLT)
        final int ctas = 2; // CREATE TABLE AS SELECT ... (CTAS)
        int commandType = createTable;
        HiveBucketSpec hiveBucketSpec = null;

        HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams rowFormatParams =
                new HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams();
        HiveParserStorageFormat storageFormat = new HiveParserStorageFormat(conf);

        LOG.info("Creating table " + dbDotTab + " position=" + ast.getCharPositionInLine());
        int numCh = ast.getChildCount();

        // Check the 1st-level children and do simple semantic checks: 1) CTLT and CTAS should not
        // coexists.
        // 2) CTLT or CTAS should not coexists with column list (target table schema).
        // 3) CTAS does not support partitioning (for now).
        for (int num = 1; num < numCh; num++) {
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(num);
            if (storageFormat.fillStorageFormat(child)) {
                continue;
            }
            switch (child.getToken().getType()) {
                case HiveASTParser.TOK_IFNOTEXISTS:
                    ifNotExists = true;
                    break;
                case HiveASTParser.KW_EXTERNAL:
                    isExt = true;
                    break;
                case HiveASTParser.KW_TEMPORARY:
                    isTemporary = true;
                    break;
                case HiveASTParser.TOK_LIKETABLE:
                    if (child.getChildCount() > 0) {
                        likeTableName =
                                HiveParserBaseSemanticAnalyzer.getUnescapedName(
                                        (HiveParserASTNode) child.getChild(0));
                        if (likeTableName != null) {
                            if (commandType == ctas) {
                                throw new ValidationException(
                                        ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
                            }
                            if (cols.size() != 0) {
                                throw new ValidationException(
                                        ErrorMsg.CTLT_COLLST_COEXISTENCE.getMsg());
                            }
                        }
                        commandType = ctlt;
                        handleUnsupportedOperation("CREATE TABLE LIKE is not supported");
                    }
                    break;

                case HiveASTParser.TOK_QUERY: // CTAS
                    if (commandType == ctlt) {
                        throw new ValidationException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
                    }
                    if (cols.size() != 0) {
                        throw new ValidationException(ErrorMsg.CTAS_COLLST_COEXISTENCE.getMsg());
                    }
                    if (partCols.size() != 0) {
                        throw new ValidationException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
                    }
                    if (isExt) {
                        throw new ValidationException(ErrorMsg.CTAS_EXTTBL_COEXISTENCE.getMsg());
                    }
                    commandType = ctas;
                    selectStmt = child;
                    break;
                case HiveASTParser.TOK_TABCOLLIST:
                    cols =
                            HiveParserBaseSemanticAnalyzer.getColumns(
                                    child, true, primaryKeys, notNulls);
                    break;
                case HiveASTParser.TOK_TABLECOMMENT:
                    comment =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(0).getText());
                    break;
                case HiveASTParser.TOK_TABLEPARTCOLS:
                    partCols =
                            HiveParserBaseSemanticAnalyzer.getColumns(
                                    (HiveParserASTNode) child.getChild(0), false);
                    break;
                case HiveASTParser.TOK_TABLEROWFORMAT:
                    rowFormatParams.analyzeRowFormat(child);
                    break;
                case HiveASTParser.TOK_TABLELOCATION:
                    location =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(0).getText());
                    break;
                case HiveASTParser.TOK_TABLEPROPERTIES:
                    tblProps = getProps((HiveParserASTNode) child.getChild(0));
                    break;
                case HiveASTParser.TOK_TABLESERIALIZER:
                    child = (HiveParserASTNode) child.getChild(0);
                    storageFormat.setSerde(
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(0).getText()));
                    if (child.getChildCount() == 2) {
                        HiveParserBaseSemanticAnalyzer.readProps(
                                (HiveParserASTNode) (child.getChild(1).getChild(0)),
                                storageFormat.getSerdeProps());
                    }
                    break;
                case HiveASTParser.TOK_ALTERTABLE_BUCKETS:
                    hiveBucketSpec = getBucketSpec(child);
                    break;
                case HiveASTParser.TOK_TABLESKEWED:
                    handleUnsupportedOperation("Skewed table is not supported");
                    break;
                default:
                    throw new ValidationException("Unknown AST node for CREATE TABLE: " + child);
            }
        }

        if (storageFormat.getStorageHandler() != null) {
            handleUnsupportedOperation("Storage handler table is not supported");
        }

        if (commandType == createTable || commandType == ctlt) {
            queryState.setCommandType(HiveOperation.CREATETABLE);
        } else {
            queryState.setCommandType(HiveOperation.CREATETABLE_AS_SELECT);
        }

        storageFormat.fillDefaultStorageFormat(isExt, false);

        if (isTemporary) {
            if (partCols.size() > 0) {
                handleUnsupportedOperation(
                        "Partition columns are not supported on temporary tables");
            }
            handleUnsupportedOperation("Temporary hive table is not supported");
        }

        // Handle different types of CREATE TABLE command
        switch (commandType) {
            case createTable: // REGULAR CREATE TABLE DDL
                tblProps = addDefaultProperties(tblProps);
                return convertCreateTable(
                        dbDotTab,
                        isExt,
                        ifNotExists,
                        isTemporary,
                        cols,
                        partCols,
                        comment,
                        location,
                        tblProps,
                        rowFormatParams,
                        storageFormat,
                        primaryKeys,
                        notNulls,
                        hiveBucketSpec);

            case ctlt: // create table like <tbl_name>
                tblProps = addDefaultProperties(tblProps);
                throw new SemanticException("CREATE TABLE LIKE is not supported yet");

            case ctas: // create table as select
                tblProps = addDefaultProperties(tblProps);

                // analyze the query
                HiveParserCalcitePlanner calcitePlanner =
                        hiveParser.createCalcitePlanner(context, queryState, hiveShim);
                calcitePlanner.setCtasCols(cols);
                RelNode queryRelNode = calcitePlanner.genLogicalPlan(selectStmt);
                // create a table to represent the dest table
                String[] dbTblName = dbDotTab.split("\\.");
                Table destTable = new Table(Table.getEmptyTable(dbTblName[0], dbTblName[1]));
                destTable.getSd().setCols(cols);

                Tuple4<ObjectIdentifier, QueryOperation, Map<String, String>, Boolean>
                        insertOperationInfo =
                                dmlHelper.createInsertOperationInfo(
                                        queryRelNode,
                                        destTable,
                                        Collections.emptyMap(),
                                        Collections.emptyList(),
                                        false);

                CreateTableOperation createTableOperation =
                        convertCreateTable(
                                dbDotTab,
                                isExt,
                                ifNotExists,
                                isTemporary,
                                cols,
                                partCols,
                                comment,
                                location,
                                tblProps,
                                rowFormatParams,
                                storageFormat,
                                primaryKeys,
                                notNulls,
                                hiveBucketSpec);

                return new CreateTableASOperation(
                        createTableOperation,
                        insertOperationInfo.f2,
                        insertOperationInfo.f1,
                        insertOperationInfo.f3);
            default:
                throw new ValidationException("Unrecognized command.");
        }
    }

    private CreateTableOperation convertCreateTable(
            String compoundName,
            boolean isExternal,
            boolean ifNotExists,
            boolean isTemporary,
            List<FieldSchema> cols,
            List<FieldSchema> partCols,
            String comment,
            String location,
            Map<String, String> tblProps,
            HiveParserRowFormatParams rowFormatParams,
            HiveParserStorageFormat storageFormat,
            List<PrimaryKey> primaryKeys,
            List<NotNullConstraint> notNullConstraints,
            HiveBucketSpec hiveBucketSpec)
            throws SemanticException {
        Map<String, String> props = new HashMap<>();
        if (tblProps != null) {
            props.putAll(tblProps);
        }
        markHiveConnector(props);
        // external
        if (isExternal) {
            props.put(TABLE_IS_EXTERNAL, "true");
        }
        // PK trait
        UniqueConstraint uniqueConstraint = null;
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            PrimaryKey primaryKey = primaryKeys.get(0);
            byte trait = 0;
            if (primaryKey.isEnable()) {
                trait = HiveDDLUtils.enableConstraint(trait);
            }
            if (primaryKey.isValidate()) {
                trait = HiveDDLUtils.validateConstraint(trait);
            }
            if (primaryKey.isRely()) {
                trait = HiveDDLUtils.relyConstraint(trait);
            }
            props.put(PK_CONSTRAINT_TRAIT, String.valueOf(trait));
            List<String> pkCols =
                    primaryKeys.stream().map(PrimaryKey::getPk).collect(Collectors.toList());
            String constraintName = primaryKey.getConstraintName();
            if (constraintName == null) {
                constraintName = pkCols.stream().collect(Collectors.joining("_", "PK_", ""));
            }
            uniqueConstraint = UniqueConstraint.primaryKey(constraintName, pkCols);
        }
        // NOT NULL constraints
        List<String> notNullCols = new ArrayList<>();
        if (!notNullConstraints.isEmpty()) {
            List<String> traits = new ArrayList<>();
            for (NotNullConstraint notNull : notNullConstraints) {
                byte trait = 0;
                if (notNull.isEnable()) {
                    trait = HiveDDLUtils.enableConstraint(trait);
                }
                if (notNull.isValidate()) {
                    trait = HiveDDLUtils.validateConstraint(trait);
                }
                if (notNull.isRely()) {
                    trait = HiveDDLUtils.relyConstraint(trait);
                }
                traits.add(String.valueOf(trait));
                notNullCols.add(notNull.getColName());
            }
            props.put(NOT_NULL_CONSTRAINT_TRAITS, String.join(COL_DELIMITER, traits));
            props.put(NOT_NULL_COLS, String.join(COL_DELIMITER, notNullCols));
        }
        // row format
        if (rowFormatParams != null) {
            encodeRowFormat(rowFormatParams, props);
        }
        // storage format
        if (storageFormat != null) {
            encodeStorageFormat(storageFormat, props);
        }
        // location
        if (location != null) {
            props.put(TABLE_LOCATION_URI, location);
        }

        // bucket spec
        if (hiveBucketSpec != null) {
            Set<String> allCols = new HashSet<>();
            for (FieldSchema fieldSchema : cols) {
                allCols.add(fieldSchema.getName());
            }
            for (FieldSchema fieldSchema : partCols) {
                allCols.add(fieldSchema.getName());
            }
            for (String bucketCol :
                    hiveBucketSpec.getBucketColumns().orElse(Collections.emptyList())) {
                if (!allCols.contains(bucketCol)) {
                    throw new SemanticException(
                            String.format("Invalid col reference %s for clustered by.", bucketCol));
                }
            }

            for (String sortedCol :
                    hiveBucketSpec
                            .getColumnNamesOrder()
                            .orElse(Tuple2.of(Collections.emptyList(), Collections.emptyList()))
                            .f0) {
                if (!allCols.contains(sortedCol)) {
                    throw new SemanticException(
                            String.format("Invalid col reference %s for sorted by.", sortedCol));
                }
            }
            hiveBucketSpec.dumpToProps(props);
        }

        ObjectIdentifier identifier = parseObjectIdentifier(compoundName);
        Set<String> notNullColSet = new HashSet<>(notNullCols);
        if (uniqueConstraint != null) {
            notNullColSet.addAll(uniqueConstraint.getColumns());
        }
        TableSchema tableSchema =
                HiveTableUtil.createTableSchema(cols, partCols, notNullColSet, uniqueConstraint);

        return new CreateTableOperation(
                identifier,
                new CatalogTableImpl(
                        tableSchema, HiveCatalog.getFieldNames(partCols), props, comment),
                ifNotExists,
                isTemporary);
    }

    private void markHiveConnector(Map<String, String> props) {
        props.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);
    }

    private void encodeRowFormat(
            HiveParserRowFormatParams rowFormatParams, Map<String, String> props) {
        if (rowFormatParams.getFieldDelim() != null) {
            props.put(FIELD_DELIM, rowFormatParams.getFieldDelim());
        }
        if (rowFormatParams.getCollItemDelim() != null) {
            props.put(COLLECTION_DELIM, rowFormatParams.getCollItemDelim());
        }
        if (rowFormatParams.getMapKeyDelim() != null) {
            props.put(MAPKEY_DELIM, rowFormatParams.getMapKeyDelim());
        }
        if (rowFormatParams.getFieldEscape() != null) {
            props.put(ESCAPE_CHAR, rowFormatParams.getFieldEscape());
        }
        if (rowFormatParams.getLineDelim() != null) {
            props.put(LINE_DELIM, rowFormatParams.getLineDelim());
        }
        if (rowFormatParams.getNullFormat() != null) {
            props.put(SERIALIZATION_NULL_FORMAT, rowFormatParams.getNullFormat());
        }
    }

    private void encodeStorageFormat(
            HiveParserStorageFormat storageFormat, Map<String, String> props) {
        String serdeName = storageFormat.getSerde();
        if (serdeName != null) {
            props.put(SERDE_LIB_CLASS_NAME, serdeName);
        }
        Map<String, String> serdeProps = storageFormat.getSerdeProps();
        if (serdeProps != null) {
            for (String serdeKey : serdeProps.keySet()) {
                props.put(SERDE_INFO_PROP_PREFIX + serdeKey, serdeProps.get(serdeKey));
            }
        }
        if (storageFormat.getInputFormat() != null) {
            props.put(STORED_AS_INPUT_FORMAT, storageFormat.getInputFormat());
        }
        if (storageFormat.getOutputFormat() != null) {
            props.put(STORED_AS_OUTPUT_FORMAT, storageFormat.getOutputFormat());
        }
    }

    private Operation convertAlterDatabaseProperties(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
        Map<String, String> dbProps = null;

        for (int i = 1; i < ast.getChildCount(); i++) {
            HiveParserASTNode childNode = (HiveParserASTNode) ast.getChild(i);
            if (childNode.getToken().getType() == HiveASTParser.TOK_DATABASEPROPERTIES) {
                dbProps = getProps((HiveParserASTNode) childNode.getChild(0));
            } else {
                throw new ValidationException(
                        "Unknown AST node for ALTER DATABASE PROPERTIES: " + childNode);
            }
        }
        CatalogDatabase originDB = getDatabase(dbName);
        Map<String, String> props = new HashMap<>(originDB.getProperties());
        props.put(ALTER_DATABASE_OP, SqlAlterHiveDatabase.AlterHiveDatabaseOp.CHANGE_PROPS.name());
        props.putAll(dbProps);
        CatalogDatabase newDB = new CatalogDatabaseImpl(props, originDB.getComment());
        return new AlterDatabaseOperation(catalogManager.getCurrentCatalog(), dbName, newDB);
    }

    private Operation convertAlterDatabaseOwner(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.getUnescapedName(
                        (HiveParserASTNode) ast.getChild(0));
        PrincipalDesc principalDesc =
                HiveParserAuthorizationParseUtils.getPrincipalDesc(
                        (HiveParserASTNode) ast.getChild(1));

        // The syntax should not allow these fields to be null, but lets verify
        String nullCmdMsg = "can't be null in alter database set owner command";
        if (principalDesc.getName() == null) {
            throw new ValidationException("Owner name " + nullCmdMsg);
        }
        if (principalDesc.getType() == null) {
            throw new ValidationException("Owner type " + nullCmdMsg);
        }
        CatalogDatabase originDB = getDatabase(dbName);
        Map<String, String> props = new HashMap<>(originDB.getProperties());
        props.put(ALTER_DATABASE_OP, SqlAlterHiveDatabase.AlterHiveDatabaseOp.CHANGE_OWNER.name());
        props.put(DATABASE_OWNER_NAME, principalDesc.getName());
        props.put(DATABASE_OWNER_TYPE, principalDesc.getType().name().toLowerCase());
        CatalogDatabase newDB = new CatalogDatabaseImpl(props, originDB.getComment());
        return new AlterDatabaseOperation(catalogManager.getCurrentCatalog(), dbName, newDB);
    }

    private Operation convertAlterDatabaseLocation(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.getUnescapedName(
                        (HiveParserASTNode) ast.getChild(0));
        String newLocation =
                HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(1).getText());
        CatalogDatabase originDB = getDatabase(dbName);
        Map<String, String> props = new HashMap<>(originDB.getProperties());
        props.put(
                ALTER_DATABASE_OP, SqlAlterHiveDatabase.AlterHiveDatabaseOp.CHANGE_LOCATION.name());
        props.put(DATABASE_LOCATION_URI, newLocation);
        CatalogDatabase newDB = new CatalogDatabaseImpl(props, originDB.getComment());
        return new AlterDatabaseOperation(catalogManager.getCurrentCatalog(), dbName, newDB);
    }

    private Operation convertCreateDatabase(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
        boolean ifNotExists = false;
        String dbComment = null;
        String dbLocation = null;
        Map<String, String> dbProps = null;

        for (int i = 1; i < ast.getChildCount(); i++) {
            HiveParserASTNode childNode = (HiveParserASTNode) ast.getChild(i);
            switch (childNode.getToken().getType()) {
                case HiveASTParser.TOK_IFNOTEXISTS:
                    ifNotExists = true;
                    break;
                case HiveASTParser.TOK_DATABASECOMMENT:
                    dbComment =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    childNode.getChild(0).getText());
                    break;
                case HiveASTParser.TOK_DATABASEPROPERTIES:
                    dbProps = getProps((HiveParserASTNode) childNode.getChild(0));
                    break;
                case HiveASTParser.TOK_DATABASELOCATION:
                    dbLocation =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    childNode.getChild(0).getText());
                    break;
                default:
                    throw new ValidationException(
                            "Unknown AST node for CREATE DATABASE: " + childNode);
            }
        }

        Map<String, String> props = new HashMap<>();
        if (dbProps != null) {
            props.putAll(dbProps);
        }

        if (dbLocation != null) {
            props.put(DATABASE_LOCATION_URI, dbLocation);
        }
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(props, dbComment);
        return new CreateDatabaseOperation(
                catalogManager.getCurrentCatalog(), dbName, catalogDatabase, ifNotExists);
    }

    private Operation convertDropDatabase(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
        boolean ifExists = false;
        boolean ifCascade = false;

        if (null != ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS)) {
            ifExists = true;
        }

        if (null != ast.getFirstChildWithType(HiveASTParser.TOK_CASCADE)) {
            ifCascade = true;
        }

        return new DropDatabaseOperation(
                catalogManager.getCurrentCatalog(), dbName, ifExists, ifCascade);
    }

    private Operation convertSwitchDatabase(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
        return new UseDatabaseOperation(catalogManager.getCurrentCatalog(), dbName);
    }

    private Operation convertDropTable(HiveParserASTNode ast, TableType expectedType) {
        String tableName =
                HiveParserBaseSemanticAnalyzer.getUnescapedName(
                        (HiveParserASTNode) ast.getChild(0));
        boolean ifExists = (ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS) != null);

        ObjectIdentifier identifier = parseObjectIdentifier(tableName);
        CatalogBaseTable baseTable = getCatalogBaseTable(identifier, true);

        if (expectedType == TableType.VIRTUAL_VIEW) {
            if (baseTable instanceof CatalogTable) {
                throw new ValidationException("DROP VIEW for a table is not allowed");
            }
            return new DropViewOperation(identifier, ifExists, false);
        } else {
            if (baseTable instanceof CatalogView) {
                throw new ValidationException("DROP TABLE for a view is not allowed");
            }
            return new DropTableOperation(identifier, ifExists, false);
        }
    }

    private void validateAlterTableType(Table tbl) {
        if (tbl.isNonNative()) {
            throw new ValidationException(
                    ErrorMsg.ALTER_TABLE_NON_NATIVE.getMsg(tbl.getTableName()));
        }
    }

    private Operation convertAlterTableProps(
            CatalogBaseTable alteredTable,
            String tableName,
            HashMap<String, String> partSpec,
            HiveParserASTNode ast,
            boolean expectView,
            boolean isUnset) {

        HashMap<String, String> mapProp =
                getProps((HiveParserASTNode) (ast.getChild(0)).getChild(0));
        // we need to check if the properties are valid, especially for stats.
        // they might be changed via alter table .. update statistics or alter table .. set
        // tblproperties.
        // If the property is not row_count or raw_data_size, it could not be changed through update
        // statistics
        for (Map.Entry<String, String> entry : mapProp.entrySet()) {
            // we make sure that we do not change anything if there is anything wrong.
            if (entry.getKey().equals(StatsSetupConst.ROW_COUNT)
                    || entry.getKey().equals(StatsSetupConst.RAW_DATA_SIZE)) {
                try {
                    Long.parseLong(entry.getValue());
                } catch (Exception e) {
                    throw new ValidationException(
                            "AlterTable "
                                    + entry.getKey()
                                    + " failed with value "
                                    + entry.getValue());
                }
            } else {
                if (HiveOperation.ALTERTABLE_UPDATETABLESTATS
                                .getOperationName()
                                .equals(queryState.getCommandType())
                        || HiveOperation.ALTERTABLE_UPDATEPARTSTATS
                                .getOperationName()
                                .equals(queryState.getCommandType())) {
                    throw new ValidationException(
                            "AlterTable UpdateStats "
                                    + entry.getKey()
                                    + " failed because the only valid keys are "
                                    + StatsSetupConst.ROW_COUNT
                                    + " and "
                                    + StatsSetupConst.RAW_DATA_SIZE);
                }
            }
        }
        if (isUnset) {
            handleUnsupportedOperation("Unset properties not supported");
        }

        if (expectView) {
            return convertAlterViewProps(alteredTable, tableName, mapProp);
        } else {
            Map<String, String> newProps = new HashMap<>();
            newProps.put(ALTER_TABLE_OP, CHANGE_TBL_PROPS.name());
            newProps.putAll(mapProp);
            return convertAlterTableProps(alteredTable, tableName, partSpec, newProps);
        }
    }

    private Operation convertAlterTableProps(
            CatalogBaseTable oldBaseTable,
            String tableName,
            Map<String, String> partSpec,
            Map<String, String> newProps) {
        ObjectIdentifier tableIdentifier = parseObjectIdentifier(tableName);
        CatalogTable oldTable = (CatalogTable) oldBaseTable;
        CatalogPartitionSpec catalogPartitionSpec =
                partSpec != null ? new CatalogPartitionSpec(partSpec) : null;
        CatalogPartition catalogPartition =
                partSpec != null ? getPartition(tableIdentifier, catalogPartitionSpec) : null;

        Map<String, String> props = new HashMap<>();
        if (catalogPartition != null) {
            props.putAll(catalogPartition.getProperties());
            props.putAll(newProps);
            return new AlterPartitionPropertiesOperation(
                    tableIdentifier,
                    catalogPartitionSpec,
                    new CatalogPartitionImpl(props, catalogPartition.getComment()));
        } else {
            props.putAll(oldTable.getOptions());
            props.putAll(newProps);
            return new AlterTableOptionsOperation(tableIdentifier, oldTable.copy(props));
        }
    }

    private Operation convertAlterTableSerdeProps(
            CatalogBaseTable alteredTable,
            HiveParserASTNode ast,
            String tableName,
            HashMap<String, String> partSpec) {
        HashMap<String, String> mapProp =
                getProps((HiveParserASTNode) (ast.getChild(0)).getChild(0));
        Map<String, String> newProps = new HashMap<>();
        newProps.put(ALTER_TABLE_OP, CHANGE_SERDE_PROPS.name());
        for (String key : mapProp.keySet()) {
            newProps.put(SERDE_INFO_PROP_PREFIX + key, mapProp.get(key));
        }
        return convertAlterTableProps(alteredTable, tableName, partSpec, newProps);
    }

    private Operation convertAlterTableSerde(
            CatalogBaseTable alteredTable,
            HiveParserASTNode ast,
            String tableName,
            HashMap<String, String> partSpec) {
        String serdeName =
                HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(0).getText());
        HashMap<String, String> mapProp = null;
        if (ast.getChildCount() > 1) {
            mapProp = getProps((HiveParserASTNode) (ast.getChild(1)).getChild(0));
        }
        Map<String, String> newProps = new HashMap<>();
        newProps.put(ALTER_TABLE_OP, CHANGE_SERDE_PROPS.name());
        newProps.put(SERDE_LIB_CLASS_NAME, serdeName);
        if (mapProp != null) {
            for (String key : mapProp.keySet()) {
                newProps.put(SERDE_INFO_PROP_PREFIX + key, mapProp.get(key));
            }
        }
        return convertAlterTableProps(alteredTable, tableName, partSpec, newProps);
    }

    private Operation convertAlterTableFileFormat(
            CatalogBaseTable alteredTable,
            HiveParserASTNode ast,
            String tableName,
            HashMap<String, String> partSpec)
            throws SemanticException {

        HiveParserStorageFormat format = new HiveParserStorageFormat(conf);
        HiveParserASTNode child = (HiveParserASTNode) ast.getChild(0);

        if (!format.fillStorageFormat(child)) {
            throw new ValidationException("Unknown AST node for ALTER TABLE FILEFORMAT: " + child);
        }

        Map<String, String> newProps = new HashMap<>();
        newProps.put(ALTER_TABLE_OP, CHANGE_FILE_FORMAT.name());
        newProps.put(STORED_AS_FILE_FORMAT, format.getGenericName());
        return convertAlterTableProps(alteredTable, tableName, partSpec, newProps);
    }

    private Operation convertAlterTableLocation(
            CatalogBaseTable alteredTable,
            HiveParserASTNode ast,
            String tableName,
            HashMap<String, String> partSpec) {
        String newLocation =
                HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(0).getText());
        Map<String, String> newProps = new HashMap<>();
        newProps.put(ALTER_TABLE_OP, CHANGE_LOCATION.name());
        newProps.put(TABLE_LOCATION_URI, newLocation);

        return convertAlterTableProps(alteredTable, tableName, partSpec, newProps);
    }

    private Operation convertAlterTableBucketNum(
            CatalogBaseTable alteredTable,
            HiveParserASTNode ast,
            String tableName,
            HashMap<String, String> partSpec) {
        int numberOfBuckets = Integer.parseInt(ast.getChild(0).getText());
        Map<String, String> newProps = new HashMap<>();
        newProps.put(ALTER_TABLE_OP, ALTER_BUCKET.name());
        dumpBucketNumToProp(numberOfBuckets, newProps);
        return convertAlterTableProps(alteredTable, tableName, partSpec, newProps);
    }

    private Operation convertAlterTableClusterSort(
            CatalogBaseTable alteredTable,
            HiveParserASTNode ast,
            String tableName,
            HashMap<String, String> partSpec)
            throws SemanticException {
        Map<String, String> newProps = new HashMap<>();
        newProps.put(ALTER_TABLE_OP, ALTER_BUCKET.name());
        switch (ast.getChild(0).getType()) {
            case HiveASTParser.TOK_NOT_CLUSTERED:
                // set the bucket columns to empty list
                HiveBucketSpec.dumpBucketColumnsToProp(Collections.emptyList(), newProps);
                // -1 buckets means to turn off bucketing
                HiveBucketSpec.dumpBucketNumToProp(-1, newProps);
                // set the sort columns to empty list
                HiveBucketSpec.dumpSortColumnsToProp(
                        Tuple2.of(Collections.emptyList(), Collections.emptyList()), newProps);
                return convertAlterTableProps(alteredTable, tableName, partSpec, newProps);
            case HiveASTParser.TOK_NOT_SORTED:
                // set the sort columns to empty list
                HiveBucketSpec.dumpSortColumnsToProp(
                        Tuple2.of(Collections.emptyList(), Collections.emptyList()), newProps);
                return convertAlterTableProps(alteredTable, tableName, partSpec, newProps);
            case HiveASTParser.TOK_ALTERTABLE_BUCKETS:
                HiveBucketSpec hiveBucketSpec = getBucketSpec((HiveParserASTNode) ast.getChild(0));
                // validate sort columns and bucket columns
                List<String> columns = Arrays.asList(alteredTable.getSchema().getFieldNames());
                Utilities.validateColumnNames(
                        columns, hiveBucketSpec.getBucketColumns().orElse(Collections.emptyList()));
                if (hiveBucketSpec.getColumnNamesOrder().isPresent()) {
                    Utilities.validateColumnNames(
                            columns, hiveBucketSpec.getColumnNamesOrder().get().f0);
                }

                hiveBucketSpec.dumpToProps(newProps);
                return convertAlterTableProps(alteredTable, tableName, partSpec, newProps);
            default:
                throw new SemanticException("Invalid operation " + ast.getChild(0).getType());
        }
    }

    public static HashMap<String, String> getProps(HiveParserASTNode prop) {
        // Must be deterministic order map for consistent q-test output across Java versions
        HashMap<String, String> mapProp = new LinkedHashMap<>();
        HiveParserBaseSemanticAnalyzer.readProps(prop, mapProp);
        return mapProp;
    }

    /** Utility class to resolve QualifiedName. */
    private static class QualifiedNameUtil {

        // Get the fully qualified name in the ast. e.g. the ast of the form ^(DOT^(DOT a b) c) will
        // generate a name of the form a.b.c
        public static String getFullyQualifiedName(HiveParserASTNode ast) {
            if (ast.getChildCount() == 0) {
                return ast.getText();
            } else if (ast.getChildCount() == 2) {
                return getFullyQualifiedName((HiveParserASTNode) ast.getChild(0))
                        + "."
                        + getFullyQualifiedName((HiveParserASTNode) ast.getChild(1));
            } else if (ast.getChildCount() == 3) {
                return getFullyQualifiedName((HiveParserASTNode) ast.getChild(0))
                        + "."
                        + getFullyQualifiedName((HiveParserASTNode) ast.getChild(1))
                        + "."
                        + getFullyQualifiedName((HiveParserASTNode) ast.getChild(2));
            } else {
                return null;
            }
        }

        // get the column path
        // return column name if exists, column could be DOT separated.
        // example: lintString.$elem$.myint
        // return table name for column name if no column has been specified.
        public static String getColPath(
                HiveParserASTNode node,
                String dbName,
                String tableName,
                Map<String, String> partSpec) {

            // if this ast has only one child, then no column name specified.
            if (node.getChildCount() == 1) {
                return tableName;
            }

            HiveParserASTNode columnNode = null;
            // Second child node could be partitionspec or column
            if (node.getChildCount() > 1) {
                if (partSpec == null) {
                    columnNode = (HiveParserASTNode) node.getChild(1);
                } else {
                    columnNode = (HiveParserASTNode) node.getChild(2);
                }
            }

            if (columnNode != null) {
                if (dbName == null) {
                    return tableName + "." + QualifiedNameUtil.getFullyQualifiedName(columnNode);
                } else {
                    return tableName.substring(dbName.length() + 1)
                            + "."
                            + QualifiedNameUtil.getFullyQualifiedName(columnNode);
                }
            } else {
                return tableName;
            }
        }

        // get partition metadata
        public static Map<String, String> getPartitionSpec(HiveParserASTNode ast) {
            HiveParserASTNode partNode = null;
            // if this ast has only one child, then no partition spec specified.
            if (ast.getChildCount() == 1) {
                return null;
            }

            // if ast has two children
            // the 2nd child could be partition spec or columnName
            // if the ast has 3 children, the second *has to* be partition spec
            if (ast.getChildCount() > 2
                    && (ast.getChild(1).getType() != HiveASTParser.TOK_PARTSPEC)) {
                throw new ValidationException(
                        ast.getChild(1).getType() + " is not a partition specification");
            }

            if (ast.getChild(1).getType() == HiveASTParser.TOK_PARTSPEC) {
                partNode = (HiveParserASTNode) ast.getChild(1);
            }

            if (partNode != null) {
                return getPartSpec(partNode);
            }

            return null;
        }
    }

    private CatalogPartition getPartition(
            ObjectIdentifier tableIdentifier, CatalogPartitionSpec partitionSpec) {
        return catalogManager
                .getPartition(tableIdentifier, partitionSpec)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        String.format(
                                                "Partition %s of table %s doesn't exist",
                                                partitionSpec.getPartitionSpec(),
                                                tableIdentifier)));
    }

    /**
     * A query like this will generate a tree as follows "describe formatted default.maptable
     * partition (b=100) id;" TOK_TABTYPE TOK_TABNAME --> root for tablename, 2 child nodes mean DB
     * specified default maptable TOK_PARTSPEC --> root node for partition spec. else columnName
     * TOK_PARTVAL b 100 id --> root node for columnName formatted
     */
    private Operation convertDescribeTable(HiveParserASTNode ast) {
        HiveParserASTNode tableTypeExpr = (HiveParserASTNode) ast.getChild(0);

        String dbName = null;
        String tableName;
        String colPath;
        Map<String, String> partSpec;

        HiveParserASTNode tableNode;

        // process the first node to extract tablename
        // tablename is either TABLENAME or DBNAME.TABLENAME if db is given
        if (tableTypeExpr.getChild(0).getType() == HiveASTParser.TOK_TABNAME) {
            tableNode = (HiveParserASTNode) tableTypeExpr.getChild(0);
            if (tableNode.getChildCount() == 1) {
                tableName = tableNode.getChild(0).getText();
            } else {
                dbName = tableNode.getChild(0).getText();
                tableName = dbName + "." + tableNode.getChild(1).getText();
            }
        } else {
            throw new ValidationException(
                    tableTypeExpr.getChild(0).getText() + " is not an expected token type");
        }

        // process the second child,if exists, node to get partition spec(s)
        partSpec = QualifiedNameUtil.getPartitionSpec(tableTypeExpr);

        // process the third child node,if exists, to get partition spec(s)
        colPath = QualifiedNameUtil.getColPath(tableTypeExpr, dbName, tableName, partSpec);

        if (partSpec != null) {
            handleUnsupportedOperation("DESCRIBE PARTITION is not supported");
        }
        if (!colPath.equals(tableName)) {
            handleUnsupportedOperation("DESCRIBE COLUMNS is not supported");
        }

        boolean isExt = false;
        boolean isFormatted = false;
        if (ast.getChildCount() == 2) {
            int descOptions = ast.getChild(1).getType();
            isExt = descOptions == HiveASTParser.KW_EXTENDED;
            isFormatted = descOptions == HiveASTParser.KW_FORMATTED;
            if (descOptions == HiveASTParser.KW_PRETTY) {
                handleUnsupportedOperation("DESCRIBE PRETTY is not supported.");
            }
        }

        ObjectIdentifier tableIdentifier = parseObjectIdentifier(tableName);
        return new DescribeTableOperation(tableIdentifier, isExt || isFormatted);
    }

    public static HashMap<String, String> getPartSpec(HiveParserASTNode partspec) {
        if (partspec == null) {
            return null;
        }
        HashMap<String, String> partSpec = new LinkedHashMap<>();
        for (int i = 0; i < partspec.getChildCount(); ++i) {
            HiveParserASTNode partVal = (HiveParserASTNode) partspec.getChild(i);
            String key = partVal.getChild(0).getText();
            String val = null;
            if (partVal.getChildCount() == 3) {
                val = HiveParserBaseSemanticAnalyzer.stripQuotes(partVal.getChild(2).getText());
            } else if (partVal.getChildCount() == 2) {
                val = HiveParserBaseSemanticAnalyzer.stripQuotes(partVal.getChild(1).getText());
            }
            partSpec.put(key.toLowerCase(), val);
        }
        return partSpec;
    }

    // Get the partition specs from the tree
    private List<Map<String, String>> getPartitionSpecs(CommonTree ast) {
        List<Map<String, String>> partSpecs = new ArrayList<>();
        // get partition metadata if partition specified
        for (int childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
            HiveParserASTNode partSpecNode = (HiveParserASTNode) ast.getChild(childIndex);
            // sanity check
            if (partSpecNode.getType() == HiveASTParser.TOK_PARTSPEC) {
                Map<String, String> partSpec = getPartSpec(partSpecNode);
                partSpecs.add(partSpec);
            }
        }
        return partSpecs;
    }

    private Operation convertShowPartitions(HiveParserASTNode ast) {
        String tableName =
                HiveParserBaseSemanticAnalyzer.getUnescapedName(
                        (HiveParserASTNode) ast.getChild(0));
        List<Map<String, String>> partSpecs = getPartitionSpecs(ast);
        // We only can have a single partition spec
        assert (partSpecs.size() <= 1);
        Map<String, String> partSpec = null;
        if (partSpecs.size() > 0) {
            partSpec = partSpecs.get(0);
        }

        ObjectIdentifier tableIdentifier = parseObjectIdentifier(tableName);
        CatalogPartitionSpec spec = null;
        if (partSpec != null && !partSpec.isEmpty()) {
            spec = new CatalogPartitionSpec(new HashMap<>(partSpec));
        }
        return new ShowPartitionsOperation(tableIdentifier, spec);
    }

    private Operation convertShowDatabases() {
        return new ShowDatabasesOperation();
    }

    private Operation convertShowTables(HiveParserASTNode ast, boolean expectView) {
        String dbName = currentDB;
        String pattern = null;

        if (ast.getChildCount() > 3) {
            throw new ValidationException("Internal error : Invalid AST " + ast.toStringTree());
        }

        switch (ast.getChildCount()) {
            case 1: // Uses a pattern
                pattern =
                        HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(0).getText());
                break;
            case 2: // Specifies a DB
                assert (ast.getChild(0).getType() == HiveASTParser.TOK_FROM);
                dbName =
                        HiveParserBaseSemanticAnalyzer.unescapeIdentifier(
                                ast.getChild(1).getText());
                break;
            case 3: // Uses a pattern and specifies a DB
                assert (ast.getChild(0).getType() == HiveASTParser.TOK_FROM);
                dbName =
                        HiveParserBaseSemanticAnalyzer.unescapeIdentifier(
                                ast.getChild(1).getText());
                pattern =
                        HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(2).getText());
                break;
            default: // No pattern or DB
                break;
        }
        if (!dbName.equalsIgnoreCase(currentDB)) {
            handleUnsupportedOperation("SHOW TABLES/VIEWS IN DATABASE is not supported");
        }
        if (pattern != null) {
            handleUnsupportedOperation("SHOW TABLES/VIEWS LIKE is not supported");
        }
        return expectView ? new ShowViewsOperation() : new ShowTablesOperation();
    }

    /**
     * Add the task according to the parsed command tree. This is used for the CLI command "SHOW
     * FUNCTIONS;".
     *
     * @param ast The parsed command tree.
     */
    private Operation convertShowFunctions(HiveParserASTNode ast) {
        if (ast.getChildCount() == 2) {
            assert (ast.getChild(0).getType() == HiveASTParser.KW_LIKE);
            throw new ValidationException("SHOW FUNCTIONS LIKE is not supported yet");
        }
        return new ShowFunctionsOperation();
    }

    private Operation convertAlterTableRename(
            String sourceName, HiveParserASTNode ast, boolean expectView) throws SemanticException {
        String[] target =
                HiveParserBaseSemanticAnalyzer.getQualifiedTableName(
                        (HiveParserASTNode) ast.getChild(0));

        String targetName = HiveParserBaseSemanticAnalyzer.getDotName(target);
        ObjectIdentifier objectIdentifier = parseObjectIdentifier(sourceName);

        return expectView
                ? new AlterViewRenameOperation(objectIdentifier, parseObjectIdentifier(targetName))
                : new AlterTableRenameOperation(
                        objectIdentifier, parseObjectIdentifier(targetName));
    }

    private Operation convertAlterTableChangeCol(
            CatalogBaseTable alteredTable, String[] qualified, HiveParserASTNode ast)
            throws SemanticException {
        String newComment = null;
        boolean first = false;
        String flagCol = null;
        boolean isCascade = false;
        // col_old_name col_new_name column_type [COMMENT col_comment] [FIRST|AFTER column_name]
        // [CASCADE|RESTRICT]
        String oldColName = ast.getChild(0).getText();
        String newColName = ast.getChild(1).getText();
        String newType =
                HiveParserBaseSemanticAnalyzer.getTypeStringFromAST(
                        (HiveParserASTNode) ast.getChild(2));
        int childCount = ast.getChildCount();
        for (int i = 3; i < childCount; i++) {
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(i);
            switch (child.getToken().getType()) {
                case HiveASTParser.StringLiteral:
                    newComment = HiveParserBaseSemanticAnalyzer.unescapeSQLString(child.getText());
                    break;
                case HiveASTParser.TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION:
                    flagCol =
                            HiveParserBaseSemanticAnalyzer.unescapeIdentifier(
                                    child.getChild(0).getText());
                    break;
                case HiveASTParser.KW_FIRST:
                    first = true;
                    break;
                case HiveASTParser.TOK_CASCADE:
                    isCascade = true;
                    break;
                case HiveASTParser.TOK_RESTRICT:
                    break;
                default:
                    throw new ValidationException(
                            "Unsupported token: " + child.getToken() + " for alter table");
            }
        }

        // Validate the operation of renaming a column name.
        Table tab = getTable(new ObjectPath(qualified[0], qualified[1]));

        SkewedInfo skewInfo = tab.getTTable().getSd().getSkewedInfo();
        if ((null != skewInfo)
                && (null != skewInfo.getSkewedColNames())
                && skewInfo.getSkewedColNames().contains(oldColName)) {
            throw new ValidationException(
                    oldColName + ErrorMsg.ALTER_TABLE_NOT_ALLOWED_RENAME_SKEWED_COLUMN.getMsg());
        }

        String tblName = HiveParserBaseSemanticAnalyzer.getDotName(qualified);

        ObjectIdentifier tableIdentifier = parseObjectIdentifier(tblName);
        CatalogTable oldTable = (CatalogTable) alteredTable;
        String oldName = HiveParserBaseSemanticAnalyzer.unescapeIdentifier(oldColName);
        String newName = HiveParserBaseSemanticAnalyzer.unescapeIdentifier(newColName);

        if (oldTable.getPartitionKeys().contains(oldName)) {
            // disallow changing partition columns
            throw new ValidationException("CHANGE COLUMN cannot be applied to partition columns");
        }
        TableSchema oldSchema = oldTable.getSchema();
        TableColumn newTableColumn =
                TableColumn.physical(
                        newName,
                        HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(newType)));
        TableSchema newSchema =
                OperationConverterUtils.changeColumn(
                        oldSchema, oldName, newTableColumn, first, flagCol);
        Map<String, String> props = new HashMap<>(oldTable.getOptions());
        props.put(ALTER_TABLE_OP, ALTER_COLUMNS.name());
        if (isCascade) {
            props.put(ALTER_COL_CASCADE, "true");
        }
        return new AlterTableSchemaOperation(
                tableIdentifier,
                new CatalogTableImpl(
                        newSchema, oldTable.getPartitionKeys(), props, oldTable.getComment()));
    }

    private Operation convertAlterTableModifyCols(
            CatalogBaseTable alteredTable, String tblName, HiveParserASTNode ast, boolean replace)
            throws SemanticException {

        List<FieldSchema> newCols =
                HiveParserBaseSemanticAnalyzer.getColumns((HiveParserASTNode) ast.getChild(0));
        boolean isCascade = false;
        if (null != ast.getFirstChildWithType(HiveASTParser.TOK_CASCADE)) {
            isCascade = true;
        }

        ObjectIdentifier tableIdentifier = parseObjectIdentifier(tblName);
        CatalogTable oldTable = (CatalogTable) alteredTable;

        // prepare properties
        Map<String, String> props = new HashMap<>(oldTable.getOptions());
        props.put(ALTER_TABLE_OP, ALTER_COLUMNS.name());
        if (isCascade) {
            props.put(ALTER_COL_CASCADE, "true");
        }
        TableSchema oldSchema = oldTable.getSchema();
        final int numPartCol = oldTable.getPartitionKeys().size();
        TableSchema.Builder builder = TableSchema.builder();
        // add existing non-part col if we're not replacing
        if (!replace) {
            List<TableColumn> nonPartCols =
                    oldSchema.getTableColumns().subList(0, oldSchema.getFieldCount() - numPartCol);
            for (TableColumn column : nonPartCols) {
                builder.add(column);
            }
            setWatermarkAndPK(builder, oldSchema);
        }
        // add new cols
        for (FieldSchema col : newCols) {
            builder.add(
                    TableColumn.physical(
                            col.getName(),
                            HiveTypeUtil.toFlinkType(
                                    TypeInfoUtils.getTypeInfoFromTypeString(col.getType()))));
        }
        // add part cols
        List<TableColumn> partCols =
                oldSchema
                        .getTableColumns()
                        .subList(oldSchema.getFieldCount() - numPartCol, oldSchema.getFieldCount());
        for (TableColumn column : partCols) {
            builder.add(column);
        }
        return new AlterTableSchemaOperation(
                tableIdentifier,
                new CatalogTableImpl(
                        builder.build(),
                        oldTable.getPartitionKeys(),
                        props,
                        oldTable.getComment()));
    }

    private static void setWatermarkAndPK(TableSchema.Builder builder, TableSchema schema) {
        for (WatermarkSpec watermarkSpec : schema.getWatermarkSpecs()) {
            builder.watermark(watermarkSpec);
        }
        schema.getPrimaryKey()
                .ifPresent(
                        pk -> {
                            builder.primaryKey(
                                    pk.getName(), pk.getColumns().toArray(new String[0]));
                        });
    }

    private Operation convertAlterTableDropParts(String[] qualified, HiveParserASTNode ast) {

        boolean ifExists = ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS) != null;
        // If the drop has to fail on non-existent partitions, we cannot batch expressions.
        // That is because we actually have to check each separate expression for existence.
        // We could do a small optimization for the case where expr has all columns and all
        // operators are equality, if we assume those would always match one partition (which
        // may not be true with legacy, non-normalized column values). This is probably a
        // popular case but that's kinda hacky. Let's not do it for now.

        Table tab = getTable(new ObjectPath(qualified[0], qualified[1]));
        // hive represents drop partition specs with generic func desc, but what we need is just
        // spec maps
        List<Map<String, String>> partSpecs = new ArrayList<>();
        for (int i = 0; i < ast.getChildCount(); i++) {
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(i);
            if (child.getType() == HiveASTParser.TOK_PARTSPEC) {
                partSpecs.add(getPartSpec(child));
            }
        }

        validateAlterTableType(tab);

        ObjectIdentifier tableIdentifier =
                catalogManager.qualifyIdentifier(
                        UnresolvedIdentifier.of(qualified[0], qualified[1]));
        List<CatalogPartitionSpec> specs =
                partSpecs.stream().map(CatalogPartitionSpec::new).collect(Collectors.toList());
        return new DropPartitionsOperation(tableIdentifier, ifExists, specs);
    }

    /**
     * Add one or more partitions to a table. Useful when the data has been copied to the right
     * location by some other process.
     */
    private Operation convertAlterTableAddParts(String[] qualified, CommonTree ast) {
        // ^(TOK_ALTERTABLE_ADDPARTS identifier ifNotExists?
        // alterStatementSuffixAddPartitionsElement+)
        boolean ifNotExists = ast.getChild(0).getType() == HiveASTParser.TOK_IFNOTEXISTS;

        Table tab = getTable(new ObjectPath(qualified[0], qualified[1]));
        boolean isView = tab.isView();
        validateAlterTableType(tab);

        int numCh = ast.getChildCount();
        int start = ifNotExists ? 1 : 0;

        String currentLocation = null;
        Map<String, String> currentPartSpec = null;
        // Parser has done some verification, so the order of tokens doesn't need to be verified
        // here.
        List<CatalogPartitionSpec> specs = new ArrayList<>();
        List<CatalogPartition> partitions = new ArrayList<>();
        for (int num = start; num < numCh; num++) {
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(num);
            switch (child.getToken().getType()) {
                case HiveASTParser.TOK_PARTSPEC:
                    if (currentPartSpec != null) {
                        specs.add(new CatalogPartitionSpec(currentPartSpec));
                        Map<String, String> props = new HashMap<>();
                        if (currentLocation != null) {
                            props.put(TABLE_LOCATION_URI, currentLocation);
                        }
                        partitions.add(new CatalogPartitionImpl(props, null));
                        currentLocation = null;
                    }
                    currentPartSpec = getPartSpec(child);
                    validatePartitionValues(currentPartSpec); // validate reserved values
                    break;
                case HiveASTParser.TOK_PARTITIONLOCATION:
                    // if location specified, set in partition
                    if (isView) {
                        throw new ValidationException("LOCATION clause illegal for view partition");
                    }
                    currentLocation =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(0).getText());
                    break;
                default:
                    throw new ValidationException("Unknown child: " + child);
            }
        }

        // add the last one
        if (currentPartSpec != null) {
            specs.add(new CatalogPartitionSpec(currentPartSpec));
            Map<String, String> props = new HashMap<>();
            if (currentLocation != null) {
                props.put(TABLE_LOCATION_URI, currentLocation);
            }
            partitions.add(new CatalogPartitionImpl(props, null));
        }

        ObjectIdentifier tableIdentifier =
                tab.getDbName() == null
                        ? parseObjectIdentifier(tab.getTableName())
                        : catalogManager.qualifyIdentifier(
                                UnresolvedIdentifier.of(tab.getDbName(), tab.getTableName()));
        return new AddPartitionsOperation(tableIdentifier, ifNotExists, specs, partitions);
    }

    private Operation convertAlterViewProps(
            CatalogBaseTable oldBaseTable, String tableName, Map<String, String> newProps) {
        ObjectIdentifier viewIdentifier = parseObjectIdentifier(tableName);
        CatalogView oldView = (CatalogView) oldBaseTable;
        Map<String, String> props = new HashMap<>(oldView.getOptions());
        props.putAll(newProps);
        CatalogView newView =
                new CatalogViewImpl(
                        oldView.getOriginalQuery(),
                        oldView.getExpandedQuery(),
                        oldView.getSchema(),
                        props,
                        oldView.getComment());
        return new AlterViewPropertiesOperation(viewIdentifier, newView);
    }

    private CatalogBaseTable getAlteredTable(String tableName, boolean expectView) {
        ObjectIdentifier objectIdentifier = parseObjectIdentifier(tableName);
        CatalogBaseTable catalogBaseTable = getCatalogBaseTable(objectIdentifier);
        if (expectView) {
            if (catalogBaseTable instanceof CatalogTable) {
                throw new ValidationException("ALTER VIEW for a table is not allowed");
            }
        } else {
            if (catalogBaseTable instanceof CatalogView) {
                throw new ValidationException("ALTER TABLE for a view is not allowed");
            }
        }
        return catalogBaseTable;
    }

    private ObjectIdentifier parseObjectIdentifier(String compoundName) {
        UnresolvedIdentifier unresolvedIdentifier = hiveParser.parseIdentifier(compoundName);
        return catalogManager.qualifyIdentifier(unresolvedIdentifier);
    }

    private CatalogDatabase getDatabase(String databaseName) {
        Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();
        CatalogDatabase database;
        try {
            database = catalog.getDatabase(databaseName);
        } catch (DatabaseNotExistException e) {
            throw new ValidationException(String.format("Database %s not exists", databaseName), e);
        }
        return database;
    }

    private CatalogBaseTable getCatalogBaseTable(ObjectIdentifier tableIdentifier) {
        return getCatalogBaseTable(tableIdentifier, false);
    }

    private CatalogBaseTable getCatalogBaseTable(
            ObjectIdentifier tableIdentifier, boolean ifExists) {
        Optional<ContextResolvedTable> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);
        if (!optionalCatalogTable.isPresent()) {
            if (ifExists) {
                return null;
            } else {
                throw new ValidationException(
                        String.format(
                                "Table or View %s doesn't exist.", tableIdentifier.toString()));
            }
        }
        if (optionalCatalogTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format("Table or View %s is temporary.", tableIdentifier.toString()));
        }
        return optionalCatalogTable.get().getTable();
    }

    /**
     * Certain partition values are are used by hive. e.g. the default partition in dynamic
     * partitioning and the intermediate partition values used in the archiving process. Naturally,
     * prohibit the user from creating partitions with these reserved values. The check that this
     * function is more restrictive than the actual limitation, but it's simpler. Should be okay
     * since the reserved names are fairly long and uncommon.
     */
    private void validatePartitionValues(Map<String, String> partSpec) {
        for (Map.Entry<String, String> e : partSpec.entrySet()) {
            for (String s : reservedPartitionValues) {
                String value = e.getValue();
                if (value != null && value.contains(s)) {
                    throw new ValidationException(
                            ErrorMsg.RESERVED_PART_VAL.getMsg(
                                    "(User value: "
                                            + e.getValue()
                                            + " Reserved substring: "
                                            + s
                                            + ")"));
                }
            }
        }
    }

    private Map<String, String> addDefaultProperties(Map<String, String> tblProp) {
        Map<String, String> retValue;
        if (tblProp == null) {
            retValue = new HashMap<>();
        } else {
            retValue = tblProp;
        }
        String paraString = HiveConf.getVar(conf, HiveConf.ConfVars.NEWTABLEDEFAULTPARA);
        if (paraString != null && !paraString.isEmpty()) {
            for (String keyValuePair : paraString.split(",")) {
                String[] keyValue = keyValuePair.split("=", 2);
                if (keyValue.length != 2) {
                    continue;
                }
                if (!retValue.containsKey(keyValue[0])) {
                    retValue.put(keyValue[0], keyValue[1]);
                }
            }
        }
        return retValue;
    }

    private static void handleUnsupportedOperation(HiveParserASTNode astNode) {
        throw new ValidationException(
                null, new UnsupportedOperationException("Unsupported operation: " + astNode));
    }

    private static void handleUnsupportedOperation(String message) {
        throw new ValidationException(null, new UnsupportedOperationException(message));
    }
}
