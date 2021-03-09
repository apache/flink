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

package org.apache.flink.sql.parser.hive.ddl;

import org.apache.flink.sql.parser.SqlProperty;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.hive.impl.ParseException;
import org.apache.flink.sql.parser.type.ExtendedSqlCollectionTypeNameSpec;
import org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec;
import org.apache.flink.sql.parser.type.SqlMapTypeNameSpec;
import org.apache.flink.table.catalog.config.CatalogConfig;

import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase.ALTER_DATABASE_OP;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.ALTER_TABLE_OP;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveDatabase.DATABASE_LOCATION_URI;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.SERDE_INFO_PROP_PREFIX;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.SERDE_LIB_CLASS_NAME;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_FILE_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_INPUT_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_OUTPUT_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.NOT_NULL_CONSTRAINT_TRAITS;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.PK_CONSTRAINT_TRAIT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.TABLE_IS_EXTERNAL;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.TABLE_LOCATION_URI;

/** Util methods for Hive DDL Sql nodes. */
public class HiveDDLUtils {

    // assume ';' cannot be used in column identifiers or type names, otherwise we need to implement
    // escaping
    public static final String COL_DELIMITER = ";";

    private static final byte HIVE_CONSTRAINT_ENABLE = 1 << 2;
    private static final byte HIVE_CONSTRAINT_VALIDATE = 1 << 1;
    private static final byte HIVE_CONSTRAINT_RELY = 1;

    private static final Set<String> RESERVED_DB_PROPERTIES = new HashSet<>();
    private static final Set<String> RESERVED_TABLE_PROPERTIES = new HashSet<>();
    private static final List<String> RESERVED_TABLE_PROP_PREFIX = new ArrayList<>();

    private static final UnescapeStringLiteralShuttle UNESCAPE_SHUTTLE =
            new UnescapeStringLiteralShuttle();

    static {
        RESERVED_DB_PROPERTIES.addAll(Arrays.asList(ALTER_DATABASE_OP, DATABASE_LOCATION_URI));

        RESERVED_TABLE_PROPERTIES.addAll(
                Arrays.asList(
                        ALTER_TABLE_OP,
                        TABLE_LOCATION_URI,
                        TABLE_IS_EXTERNAL,
                        PK_CONSTRAINT_TRAIT,
                        NOT_NULL_CONSTRAINT_TRAITS,
                        STORED_AS_FILE_FORMAT,
                        STORED_AS_INPUT_FORMAT,
                        STORED_AS_OUTPUT_FORMAT,
                        SERDE_LIB_CLASS_NAME));

        RESERVED_TABLE_PROP_PREFIX.add(SERDE_INFO_PROP_PREFIX);
    }

    private HiveDDLUtils() {}

    public static SqlNodeList checkReservedDBProperties(SqlNodeList props) throws ParseException {
        return checkReservedProperties(RESERVED_DB_PROPERTIES, props, "Databases");
    }

    public static SqlNodeList checkReservedTableProperties(SqlNodeList props)
            throws ParseException {
        props = checkReservedProperties(RESERVED_TABLE_PROPERTIES, props, "Tables");
        props = checkReservedPrefix(RESERVED_TABLE_PROP_PREFIX, props, "Tables");
        return props;
    }

    public static SqlNodeList ensureNonGeneric(SqlNodeList props) throws ParseException {
        for (SqlNode node : props) {
            if (node instanceof SqlTableOption
                    && ((SqlTableOption) node)
                            .getKeyString()
                            .equalsIgnoreCase(CatalogConfig.IS_GENERIC)) {
                if (!((SqlTableOption) node).getValueString().equalsIgnoreCase("false")) {
                    throw new ParseException(
                            "Creating generic object with Hive dialect is not allowed");
                }
            }
        }
        return props;
    }

    private static SqlNodeList checkReservedPrefix(
            List<String> reserved, SqlNodeList properties, String metaType) throws ParseException {
        if (properties == null) {
            return null;
        }
        Set<String> match = new HashSet<>();
        for (SqlNode node : properties) {
            if (node instanceof SqlTableOption) {
                String key = ((SqlTableOption) node).getKeyString();
                for (String prefix : reserved) {
                    if (key.startsWith(prefix)) {
                        match.add(key);
                    }
                }
            }
        }
        if (!match.isEmpty()) {
            throw new ParseException(
                    String.format(
                            "Properties %s have reserved prefix and shouldn't be used for Hive %s",
                            match, metaType));
        }
        return properties;
    }

    private static SqlNodeList checkReservedProperties(
            Set<String> reservedProperties, SqlNodeList properties, String metaType)
            throws ParseException {
        if (properties == null) {
            return null;
        }
        Set<String> match = new HashSet<>();
        for (SqlNode node : properties) {
            if (node instanceof SqlTableOption) {
                String key = ((SqlTableOption) node).getKeyString();
                if (reservedProperties.contains(key)) {
                    match.add(key);
                }
            }
        }
        if (!match.isEmpty()) {
            throw new ParseException(
                    String.format(
                            "Properties %s are reserved and shouldn't be used for Hive %s",
                            match, metaType));
        }
        return properties;
    }

    public static SqlTableOption toTableOption(String key, SqlNode value, SqlParserPos pos) {
        return new SqlTableOption(SqlLiteral.createCharString(key, pos), value, pos);
    }

    public static SqlTableOption toTableOption(String key, String value, SqlParserPos pos) {
        return new SqlTableOption(
                SqlLiteral.createCharString(key, pos),
                SqlLiteral.createCharString(value, pos),
                pos);
    }

    public static void convertDataTypes(SqlNodeList columns) throws ParseException {
        if (columns != null) {
            for (SqlNode node : columns) {
                convertDataTypes((SqlRegularColumn) node);
            }
        }
    }

    // Check and convert data types to comply with HiveQL, e.g. TIMESTAMP and BINARY
    public static void convertDataTypes(SqlRegularColumn column) throws ParseException {
        column.setType(convertDataTypes(column.getType()));
    }

    private static SqlDataTypeSpec convertDataTypes(SqlDataTypeSpec typeSpec)
            throws ParseException {
        SqlTypeNameSpec nameSpec = typeSpec.getTypeNameSpec();
        SqlTypeNameSpec convertedNameSpec = convertDataTypes(nameSpec);
        if (nameSpec != convertedNameSpec) {
            boolean nullable = typeSpec.getNullable() == null ? true : typeSpec.getNullable();
            typeSpec =
                    new SqlDataTypeSpec(
                            convertedNameSpec,
                            typeSpec.getTimeZone(),
                            nullable,
                            typeSpec.getParserPosition());
        }
        return typeSpec;
    }

    private static SqlTypeNameSpec convertDataTypes(SqlTypeNameSpec nameSpec)
            throws ParseException {
        if (nameSpec instanceof SqlBasicTypeNameSpec) {
            SqlBasicTypeNameSpec basicNameSpec = (SqlBasicTypeNameSpec) nameSpec;
            if (basicNameSpec
                    .getTypeName()
                    .getSimple()
                    .equalsIgnoreCase(SqlTypeName.TIMESTAMP.name())) {
                if (basicNameSpec.getPrecision() < 0) {
                    nameSpec =
                            new SqlBasicTypeNameSpec(
                                    SqlTypeName.TIMESTAMP,
                                    9,
                                    basicNameSpec.getScale(),
                                    basicNameSpec.getCharSetName(),
                                    basicNameSpec.getParserPos());
                }
            } else if (basicNameSpec
                    .getTypeName()
                    .getSimple()
                    .equalsIgnoreCase(SqlTypeName.BINARY.name())) {
                if (basicNameSpec.getPrecision() < 0) {
                    nameSpec =
                            new SqlBasicTypeNameSpec(
                                    SqlTypeName.VARBINARY,
                                    Integer.MAX_VALUE,
                                    basicNameSpec.getScale(),
                                    basicNameSpec.getCharSetName(),
                                    basicNameSpec.getParserPos());
                }
            } else if (basicNameSpec
                    .getTypeName()
                    .getSimple()
                    .equalsIgnoreCase(SqlTypeName.VARCHAR.name())) {
                if (basicNameSpec.getPrecision() < 0) {
                    throw new ParseException("VARCHAR precision is mandatory");
                }
            }
        } else if (nameSpec instanceof ExtendedSqlCollectionTypeNameSpec) {
            ExtendedSqlCollectionTypeNameSpec collectionNameSpec =
                    (ExtendedSqlCollectionTypeNameSpec) nameSpec;
            SqlTypeNameSpec elementNameSpec = collectionNameSpec.getElementTypeName();
            SqlTypeNameSpec convertedElementNameSpec = convertDataTypes(elementNameSpec);
            if (convertedElementNameSpec != elementNameSpec) {
                nameSpec =
                        new ExtendedSqlCollectionTypeNameSpec(
                                convertedElementNameSpec,
                                collectionNameSpec.elementNullable(),
                                collectionNameSpec.getCollectionTypeName(),
                                collectionNameSpec.unparseAsStandard(),
                                collectionNameSpec.getParserPos());
            }
        } else if (nameSpec instanceof SqlMapTypeNameSpec) {
            SqlMapTypeNameSpec mapNameSpec = (SqlMapTypeNameSpec) nameSpec;
            SqlDataTypeSpec keyTypeSpec = mapNameSpec.getKeyType();
            SqlDataTypeSpec valTypeSpec = mapNameSpec.getValType();
            SqlDataTypeSpec convertedKeyTypeSpec = convertDataTypes(keyTypeSpec);
            SqlDataTypeSpec convertedValTypeSpec = convertDataTypes(valTypeSpec);
            if (keyTypeSpec != convertedKeyTypeSpec || valTypeSpec != convertedValTypeSpec) {
                nameSpec =
                        new SqlMapTypeNameSpec(
                                convertedKeyTypeSpec,
                                convertedValTypeSpec,
                                nameSpec.getParserPos());
            }
        } else if (nameSpec instanceof ExtendedSqlRowTypeNameSpec) {
            ExtendedSqlRowTypeNameSpec rowNameSpec = (ExtendedSqlRowTypeNameSpec) nameSpec;
            List<SqlDataTypeSpec> fieldTypeSpecs = rowNameSpec.getFieldTypes();
            List<SqlDataTypeSpec> convertedFieldTypeSpecs = new ArrayList<>(fieldTypeSpecs.size());
            boolean updated = false;
            for (SqlDataTypeSpec fieldTypeSpec : fieldTypeSpecs) {
                SqlDataTypeSpec convertedFieldTypeSpec = convertDataTypes(fieldTypeSpec);
                if (fieldTypeSpec != convertedFieldTypeSpec) {
                    updated = true;
                }
                convertedFieldTypeSpecs.add(convertedFieldTypeSpec);
            }
            if (updated) {
                nameSpec =
                        new ExtendedSqlRowTypeNameSpec(
                                nameSpec.getParserPos(),
                                rowNameSpec.getFieldNames(),
                                convertedFieldTypeSpecs,
                                rowNameSpec.getComments(),
                                rowNameSpec.unparseAsStandard());
            }
        }
        return nameSpec;
    }

    // a constraint is by default ENABLE NOVALIDATE RELY
    public static byte defaultTrait() {
        byte res = enableConstraint((byte) 0);
        res = relyConstraint(res);
        return res;
    }

    // returns a constraint trait that requires ENABLE
    public static byte enableConstraint(byte trait) {
        return (byte) (trait | HIVE_CONSTRAINT_ENABLE);
    }

    // returns a constraint trait that doesn't require ENABLE
    public static byte disableConstraint(byte trait) {
        return (byte) (trait & (~HIVE_CONSTRAINT_ENABLE));
    }

    // returns a constraint trait that requires VALIDATE
    public static byte validateConstraint(byte trait) {
        return (byte) (trait | HIVE_CONSTRAINT_VALIDATE);
    }

    // returns a constraint trait that doesn't require VALIDATE
    public static byte noValidateConstraint(byte trait) {
        return (byte) (trait & (~HIVE_CONSTRAINT_VALIDATE));
    }

    // returns a constraint trait that requires RELY
    public static byte relyConstraint(byte trait) {
        return (byte) (trait | HIVE_CONSTRAINT_RELY);
    }

    // returns a constraint trait that doesn't require RELY
    public static byte noRelyConstraint(byte trait) {
        return (byte) (trait & (~HIVE_CONSTRAINT_RELY));
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

    public static byte encodeConstraintTrait(SqlHiveConstraintTrait trait) {
        byte res = 0;
        if (trait.isEnable()) {
            res = enableConstraint(res);
        }
        if (trait.isValidate()) {
            res = validateConstraint(res);
        }
        if (trait.isRely()) {
            res = relyConstraint(res);
        }
        return res;
    }

    public static SqlNodeList deepCopyColList(SqlNodeList colList) {
        SqlNodeList res = new SqlNodeList(colList.getParserPosition());
        for (SqlNode node : colList) {
            res.add(deepCopyTableColumn((SqlRegularColumn) node));
        }
        return res;
    }

    public static SqlRegularColumn deepCopyTableColumn(SqlRegularColumn column) {
        return new SqlTableColumn.SqlRegularColumn(
                column.getParserPosition(),
                column.getName(),
                column.getComment().orElse(null),
                column.getType(),
                column.getConstraint().orElse(null));
    }

    // the input of sql-client will escape '\', unescape it so that users can write hive dialect
    public static void unescapeProperties(SqlNodeList properties) {
        if (properties != null) {
            properties.accept(UNESCAPE_SHUTTLE);
        }
    }

    public static SqlCharStringLiteral unescapeStringLiteral(SqlCharStringLiteral literal) {
        if (literal != null) {
            return (SqlCharStringLiteral) literal.accept(UNESCAPE_SHUTTLE);
        }
        return null;
    }

    public static void unescapePartitionSpec(SqlNodeList partSpec) {
        if (partSpec != null) {
            partSpec.accept(UNESCAPE_SHUTTLE);
        }
    }

    private static class UnescapeStringLiteralShuttle extends SqlShuttle {

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode unescaped = nodeList.get(i).accept(this);
                nodeList.set(i, unescaped);
            }
            return nodeList;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlProperty) {
                SqlProperty property = (SqlProperty) call;
                Comparable comparable = SqlLiteral.value(property.getValue());
                if (comparable instanceof NlsString) {
                    String val =
                            StringEscapeUtils.unescapeJava(((NlsString) comparable).getValue());
                    return new SqlProperty(
                            property.getKey(),
                            SqlLiteral.createCharString(
                                    val, property.getValue().getParserPosition()),
                            property.getParserPosition());
                }
            } else if (call instanceof SqlTableOption) {
                SqlTableOption option = (SqlTableOption) call;
                String key = StringEscapeUtils.unescapeJava(option.getKeyString());
                String val = StringEscapeUtils.unescapeJava(option.getValueString());
                SqlNode keyNode =
                        SqlLiteral.createCharString(key, option.getKey().getParserPosition());
                SqlNode valNode =
                        SqlLiteral.createCharString(val, option.getValue().getParserPosition());
                return new SqlTableOption(keyNode, valNode, option.getParserPosition());
            }
            return call;
        }

        @Override
        public SqlNode visit(SqlLiteral literal) {
            if (literal instanceof SqlCharStringLiteral) {
                SqlCharStringLiteral stringLiteral = (SqlCharStringLiteral) literal;
                String unescaped =
                        StringEscapeUtils.unescapeJava(stringLiteral.getNlsString().getValue());
                return SqlLiteral.createCharString(unescaped, stringLiteral.getParserPosition());
            }
            return literal;
        }
    }
}
