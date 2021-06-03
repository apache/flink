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

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.hive.impl.ParseException;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** CREATE Table DDL for Hive dialect. */
public class SqlCreateHiveTable extends SqlCreateTable {

    public static final String IDENTIFIER = "hive";

    public static final String TABLE_LOCATION_URI = "hive.location-uri";
    public static final String TABLE_IS_EXTERNAL = "hive.is-external";
    public static final String PK_CONSTRAINT_TRAIT = "hive.pk.constraint.trait";
    public static final String NOT_NULL_CONSTRAINT_TRAITS = "hive.not.null.constraint.traits";
    public static final String NOT_NULL_COLS = "hive.not.null.cols";

    private final HiveTableCreationContext creationContext;
    private final SqlNodeList originPropList;
    private final boolean isExternal;
    private final HiveTableRowFormat rowFormat;
    private final HiveTableStoredAs storedAs;
    private final SqlCharStringLiteral location;
    private final SqlNodeList origColList;
    private final SqlNodeList origPartColList;

    public SqlCreateHiveTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            HiveTableCreationContext creationContext,
            SqlNodeList propertyList,
            SqlNodeList partColList,
            @Nullable SqlCharStringLiteral comment,
            boolean isTemporary,
            boolean isExternal,
            HiveTableRowFormat rowFormat,
            HiveTableStoredAs storedAs,
            SqlCharStringLiteral location,
            boolean ifNotExists)
            throws ParseException {

        super(
                pos,
                tableName,
                columnList,
                creationContext.constraints,
                HiveDDLUtils.checkReservedTableProperties(propertyList),
                extractPartColIdentifiers(partColList),
                null,
                HiveDDLUtils.unescapeStringLiteral(comment),
                null,
                isTemporary,
                ifNotExists);

        HiveDDLUtils.unescapeProperties(propertyList);
        this.origColList = HiveDDLUtils.deepCopyColList(columnList);
        this.origPartColList =
                partColList != null ? HiveDDLUtils.deepCopyColList(partColList) : SqlNodeList.EMPTY;

        HiveDDLUtils.convertDataTypes(columnList);
        HiveDDLUtils.convertDataTypes(partColList);
        originPropList = new SqlNodeList(propertyList.getList(), propertyList.getParserPosition());
        // mark it as a hive table
        propertyList.add(HiveDDLUtils.toTableOption(FactoryUtil.CONNECTOR.key(), IDENTIFIER, pos));
        // set external
        this.isExternal = isExternal;
        if (isExternal) {
            propertyList.add(HiveDDLUtils.toTableOption(TABLE_IS_EXTERNAL, "true", pos));
        }
        // add partition cols to col list
        if (partColList != null) {
            for (SqlNode partCol : partColList) {
                columnList.add(partCol);
            }
        }
        // set PRIMARY KEY
        this.creationContext = creationContext;
        for (SqlTableConstraint tableConstraint : creationContext.constraints) {
            if (!tableConstraint.isPrimaryKey()) {
                throw new ParseException(
                        "Only PrimaryKey table constraint is supported at the moment");
            } else {
                // PK list is taken care of by super class, we need to set trait here
                propertyList.add(
                        HiveDDLUtils.toTableOption(
                                PK_CONSTRAINT_TRAIT,
                                String.valueOf(
                                        HiveDDLUtils.encodeConstraintTrait(
                                                creationContext.pkTrait)),
                                propertyList.getParserPosition()));
            }
        }
        // set NOT NULL
        if (creationContext.notNullTraits != null) {
            // set traits
            String notNullTraits =
                    creationContext.notNullTraits.stream()
                            .map(HiveDDLUtils::encodeConstraintTrait)
                            .map(Object::toString)
                            .collect(Collectors.joining(HiveDDLUtils.COL_DELIMITER));
            propertyList.add(
                    HiveDDLUtils.toTableOption(
                            NOT_NULL_CONSTRAINT_TRAITS,
                            notNullTraits,
                            propertyList.getParserPosition()));
            // set col names
            String notNullCols =
                    creationContext.notNullCols.stream()
                            .map(SqlIdentifier::getSimple)
                            .collect(Collectors.joining(HiveDDLUtils.COL_DELIMITER));
            propertyList.add(
                    HiveDDLUtils.toTableOption(
                            NOT_NULL_COLS, notNullCols, propertyList.getParserPosition()));
        }
        // set row format
        this.rowFormat = rowFormat;
        if (rowFormat != null) {
            for (SqlNode node : rowFormat.toPropList()) {
                propertyList.add(node);
            }
        }
        // set stored as
        this.storedAs = storedAs;
        if (storedAs != null) {
            for (SqlNode node : storedAs.toPropList()) {
                propertyList.add(node);
            }
        }
        // set location
        this.location = location;
        if (location != null) {
            propertyList.add(
                    HiveDDLUtils.toTableOption(
                            TABLE_LOCATION_URI, location, location.getParserPosition()));
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (isTemporary()) {
            writer.keyword("TEMPORARY");
        }
        if (isExternal) {
            writer.keyword("EXTERNAL");
        }
        writer.keyword("TABLE");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        getTableName().unparse(writer, leftPrec, rightPrec);
        // columns
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        unparseColumns(creationContext, origColList, writer, leftPrec, rightPrec);
        for (SqlTableConstraint tableConstraint : creationContext.constraints) {
            printIndent(writer);
            tableConstraint
                    .getConstraintNameIdentifier()
                    .ifPresent(
                            name -> {
                                writer.keyword("CONSTRAINT");
                                name.unparse(writer, leftPrec, rightPrec);
                            });
            writer.keyword("PRIMARY KEY");
            SqlWriter.Frame pkFrame = writer.startList("(", ")");
            tableConstraint.getColumns().unparse(writer, leftPrec, rightPrec);
            writer.endList(pkFrame);
            creationContext.pkTrait.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.endList(frame);
        // table comment
        getComment()
                .ifPresent(
                        c -> {
                            writer.keyword("COMMENT");
                            c.unparse(writer, leftPrec, rightPrec);
                        });
        // partitions
        if (origPartColList.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("PARTITIONED BY");
            SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
            unparseColumns(creationContext, origPartColList, writer, leftPrec, rightPrec);
            writer.newlineAndIndent();
            writer.endList(partitionedByFrame);
        }
        // row format
        unparseRowFormat(writer, leftPrec, rightPrec);
        // stored as
        unparseStoredAs(writer, leftPrec, rightPrec);
        // location
        if (location != null) {
            writer.newlineAndIndent();
            writer.keyword("LOCATION");
            location.unparse(writer, leftPrec, rightPrec);
        }
        // properties
        if (originPropList.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("TBLPROPERTIES");
            unparsePropList(originPropList, writer, leftPrec, rightPrec);
        }
    }

    private void unparseStoredAs(SqlWriter writer, int leftPrec, int rightPrec) {
        if (storedAs == null) {
            return;
        }
        writer.newlineAndIndent();
        writer.keyword("STORED AS");
        if (storedAs.fileFormat != null) {
            storedAs.fileFormat.unparse(writer, leftPrec, rightPrec);
        } else {
            writer.keyword("INPUTFORMAT");
            storedAs.intputFormat.unparse(writer, leftPrec, rightPrec);
            writer.keyword("OUTPUTFORMAT");
            storedAs.outputFormat.unparse(writer, leftPrec, rightPrec);
        }
    }

    private void unparseRowFormat(SqlWriter writer, int leftPrec, int rightPrec) {
        if (rowFormat == null) {
            return;
        }
        writer.newlineAndIndent();
        writer.keyword("ROW FORMAT");
        if (rowFormat.serdeClass != null) {
            writer.keyword("SERDE");
            rowFormat.serdeClass.unparse(writer, leftPrec, rightPrec);
            if (rowFormat.serdeProps != null) {
                writer.keyword("WITH SERDEPROPERTIES");
                unparsePropList(rowFormat.serdeProps, writer, leftPrec, rightPrec);
            }
        } else {
            writer.keyword("DELIMITED");
            SqlCharStringLiteral fieldDelim =
                    rowFormat.delimitPropToValue.get(HiveTableRowFormat.FIELD_DELIM);
            SqlCharStringLiteral escape =
                    rowFormat.delimitPropToValue.get(HiveTableRowFormat.ESCAPE_CHAR);
            if (fieldDelim != null) {
                writer.newlineAndIndent();
                writer.print("  ");
                writer.keyword("FIELDS TERMINATED BY");
                fieldDelim.unparse(writer, leftPrec, rightPrec);
                if (escape != null) {
                    writer.keyword("ESCAPED BY");
                    escape.unparse(writer, leftPrec, rightPrec);
                }
            }
            SqlCharStringLiteral collectionDelim =
                    rowFormat.delimitPropToValue.get(HiveTableRowFormat.COLLECTION_DELIM);
            if (collectionDelim != null) {
                writer.newlineAndIndent();
                writer.print("  ");
                writer.keyword("COLLECTION ITEMS TERMINATED BY");
                collectionDelim.unparse(writer, leftPrec, rightPrec);
            }
            SqlCharStringLiteral mapKeyDelim =
                    rowFormat.delimitPropToValue.get(HiveTableRowFormat.MAPKEY_DELIM);
            if (mapKeyDelim != null) {
                writer.newlineAndIndent();
                writer.print("  ");
                writer.keyword("MAP KEYS TERMINATED BY");
                mapKeyDelim.unparse(writer, leftPrec, rightPrec);
            }
            SqlCharStringLiteral lineDelim =
                    rowFormat.delimitPropToValue.get(HiveTableRowFormat.LINE_DELIM);
            if (lineDelim != null) {
                writer.newlineAndIndent();
                writer.print("  ");
                writer.keyword("LINES TERMINATED BY");
                lineDelim.unparse(writer, leftPrec, rightPrec);
            }
            SqlCharStringLiteral nullAs =
                    rowFormat.delimitPropToValue.get(HiveTableRowFormat.SERIALIZATION_NULL_FORMAT);
            if (nullAs != null) {
                writer.newlineAndIndent();
                writer.print("  ");
                writer.keyword("NULL DEFINED AS");
                nullAs.unparse(writer, leftPrec, rightPrec);
            }
        }
    }

    private void unparsePropList(
            SqlNodeList propList, SqlWriter writer, int leftPrec, int rightPrec) {
        SqlWriter.Frame withFrame = writer.startList("(", ")");
        for (SqlNode property : propList) {
            printIndent(writer);
            property.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.endList(withFrame);
    }

    private void unparseColumns(
            HiveTableCreationContext context,
            SqlNodeList columns,
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {
        List<SqlHiveConstraintTrait> notNullTraits = context.notNullTraits;
        int traitIndex = 0;
        for (SqlNode node : columns) {
            printIndent(writer);
            SqlRegularColumn column = (SqlRegularColumn) node;
            column.getName().unparse(writer, leftPrec, rightPrec);
            writer.print(" ");
            column.getType().unparse(writer, leftPrec, rightPrec);
            if (column.getType().getNullable() != null && !column.getType().getNullable()) {
                writer.keyword("NOT NULL");
                notNullTraits.get(traitIndex++).unparse(writer, leftPrec, rightPrec);
            }
            column.getComment()
                    .ifPresent(
                            c -> {
                                writer.keyword("COMMENT");
                                c.unparse(writer, leftPrec, rightPrec);
                            });
        }
    }

    // Extract the identifiers from partition col list -- that's what SqlCreateTable expects for
    // partition keys
    private static SqlNodeList extractPartColIdentifiers(SqlNodeList partCols) {
        if (partCols == null) {
            return null;
        }
        SqlNodeList res = new SqlNodeList(partCols.getParserPosition());
        for (SqlNode node : partCols) {
            SqlTableColumn partCol = (SqlTableColumn) node;
            res.add(partCol.getName());
        }
        return res;
    }

    /** Creation context for a Hive table. */
    public static class HiveTableCreationContext extends TableCreationContext {
        public SqlHiveConstraintTrait pkTrait = null;
        public List<SqlHiveConstraintTrait> notNullTraits = null;
        // PK cols are also considered not null, so we need to remember explicit NN cols
        public List<SqlIdentifier> notNullCols = null;
    }

    /** To represent STORED AS in CREATE TABLE DDL. */
    public static class HiveTableStoredAs {

        public static final String STORED_AS_FILE_FORMAT = "hive.storage.file-format";
        public static final String STORED_AS_INPUT_FORMAT = "hive.stored.as.input.format";
        public static final String STORED_AS_OUTPUT_FORMAT = "hive.stored.as.output.format";

        private final SqlParserPos pos;
        private final SqlIdentifier fileFormat;
        private final SqlCharStringLiteral intputFormat;
        private final SqlCharStringLiteral outputFormat;

        private HiveTableStoredAs(
                SqlParserPos pos,
                SqlIdentifier fileFormat,
                SqlCharStringLiteral intputFormat,
                SqlCharStringLiteral outputFormat)
                throws ParseException {
            this.pos = pos;
            this.fileFormat = fileFormat;
            this.intputFormat = intputFormat;
            this.outputFormat = outputFormat;
            validate();
        }

        private void validate() throws ParseException {
            if (fileFormat != null) {
                if (intputFormat != null || outputFormat != null) {
                    throw new ParseException(
                            "Both file format and input/output format are specified");
                }
            } else {
                if (intputFormat == null || outputFormat == null) {
                    throw new ParseException(
                            "Neither file format nor input/output format is specified");
                }
            }
        }

        public SqlNodeList toPropList() {
            SqlNodeList res = new SqlNodeList(pos);
            if (fileFormat != null) {
                res.add(
                        HiveDDLUtils.toTableOption(
                                STORED_AS_FILE_FORMAT,
                                fileFormat.getSimple(),
                                fileFormat.getParserPosition()));
            } else {
                res.add(
                        HiveDDLUtils.toTableOption(
                                STORED_AS_INPUT_FORMAT,
                                intputFormat,
                                intputFormat.getParserPosition()));
                res.add(
                        HiveDDLUtils.toTableOption(
                                STORED_AS_OUTPUT_FORMAT,
                                outputFormat,
                                outputFormat.getParserPosition()));
            }
            return res;
        }

        public static HiveTableStoredAs ofFileFormat(SqlParserPos pos, SqlIdentifier fileFormat)
                throws ParseException {
            return new HiveTableStoredAs(pos, fileFormat, null, null);
        }

        public static HiveTableStoredAs ofInputOutputFormat(
                SqlParserPos pos,
                SqlCharStringLiteral intputFormat,
                SqlCharStringLiteral outputFormat)
                throws ParseException {
            return new HiveTableStoredAs(pos, null, intputFormat, outputFormat);
        }
    }

    /** To represent ROW FORMAT in CREATE TABLE DDL. */
    public static class HiveTableRowFormat {

        public static final String SERDE_LIB_CLASS_NAME = "hive.serde.lib.class.name";
        public static final String SERDE_INFO_PROP_PREFIX = "hive.serde.info.prop.";
        public static final String FIELD_DELIM = SERDE_INFO_PROP_PREFIX + "field.delim";
        public static final String COLLECTION_DELIM = SERDE_INFO_PROP_PREFIX + "collection.delim";
        public static final String ESCAPE_CHAR = SERDE_INFO_PROP_PREFIX + "escape.delim";
        public static final String MAPKEY_DELIM = SERDE_INFO_PROP_PREFIX + "mapkey.delim";
        public static final String LINE_DELIM = SERDE_INFO_PROP_PREFIX + "line.delim";
        public static final String SERIALIZATION_NULL_FORMAT =
                SERDE_INFO_PROP_PREFIX + "serialization.null.format";

        private final SqlParserPos pos;
        private final Map<String, SqlCharStringLiteral> delimitPropToValue = new LinkedHashMap<>();
        private final SqlCharStringLiteral serdeClass;
        private final SqlNodeList serdeProps;

        private HiveTableRowFormat(
                SqlParserPos pos,
                SqlCharStringLiteral fieldsTerminator,
                SqlCharStringLiteral escape,
                SqlCharStringLiteral collectionTerminator,
                SqlCharStringLiteral mapKeyTerminator,
                SqlCharStringLiteral linesTerminator,
                SqlCharStringLiteral nullAs,
                SqlCharStringLiteral serdeClass,
                SqlNodeList serdeProps)
                throws ParseException {
            this.pos = pos;
            if (fieldsTerminator != null) {
                delimitPropToValue.put(FIELD_DELIM, fieldsTerminator);
            }
            if (escape != null) {
                delimitPropToValue.put(ESCAPE_CHAR, escape);
            }
            if (collectionTerminator != null) {
                delimitPropToValue.put(COLLECTION_DELIM, collectionTerminator);
            }
            if (mapKeyTerminator != null) {
                delimitPropToValue.put(MAPKEY_DELIM, mapKeyTerminator);
            }
            if (linesTerminator != null) {
                delimitPropToValue.put(LINE_DELIM, linesTerminator);
            }
            if (nullAs != null) {
                delimitPropToValue.put(SERIALIZATION_NULL_FORMAT, nullAs);
            }
            this.serdeClass = serdeClass;
            this.serdeProps = serdeProps;
            validate();
        }

        private void validate() throws ParseException {
            if (!delimitPropToValue.isEmpty()) {
                if (serdeClass != null || serdeProps != null) {
                    throw new ParseException("Both DELIMITED and SERDE specified");
                }
            } else {
                if (serdeClass == null) {
                    throw new ParseException("Neither DELIMITED nor SERDE specified");
                }
            }
        }

        public SqlNodeList toPropList() {
            SqlNodeList list = new SqlNodeList(pos);
            if (serdeClass != null) {
                list.add(HiveDDLUtils.toTableOption(SERDE_LIB_CLASS_NAME, serdeClass, pos));
                if (serdeProps != null) {
                    for (SqlNode sqlNode : serdeProps) {
                        SqlTableOption option = (SqlTableOption) sqlNode;
                        list.add(
                                HiveDDLUtils.toTableOption(
                                        SERDE_INFO_PROP_PREFIX + option.getKeyString(),
                                        option.getValue(),
                                        pos));
                    }
                }
            } else {
                for (String prop : delimitPropToValue.keySet()) {
                    list.add(HiveDDLUtils.toTableOption(prop, delimitPropToValue.get(prop), pos));
                }
            }
            HiveDDLUtils.unescapeProperties(list);
            return list;
        }

        public static HiveTableRowFormat withDelimited(
                SqlParserPos pos,
                SqlCharStringLiteral fieldsTerminator,
                SqlCharStringLiteral escape,
                SqlCharStringLiteral collectionTerminator,
                SqlCharStringLiteral mapKeyTerminator,
                SqlCharStringLiteral linesTerminator,
                SqlCharStringLiteral nullAs)
                throws ParseException {
            return new HiveTableRowFormat(
                    pos,
                    fieldsTerminator,
                    escape,
                    collectionTerminator,
                    mapKeyTerminator,
                    linesTerminator,
                    nullAs,
                    null,
                    null);
        }

        public static HiveTableRowFormat withSerDe(
                SqlParserPos pos, SqlCharStringLiteral serdeClass, SqlNodeList serdeProps)
                throws ParseException {
            return new HiveTableRowFormat(
                    pos, null, null, null, null, null, null, serdeClass, serdeProps);
        }
    }
}
