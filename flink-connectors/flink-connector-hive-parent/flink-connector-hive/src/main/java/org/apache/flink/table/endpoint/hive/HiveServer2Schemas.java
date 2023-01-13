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

package org.apache.flink.table.endpoint.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.util.Arrays;
import java.util.Collections;

/** Schemas for the HiveServer2 Endpoint result. */
public class HiveServer2Schemas {

    /** Schema for {@link HiveServer2Endpoint#GetCatalogs}. */
    public static final ResolvedSchema GET_CATALOGS_SCHEMA =
            buildSchema(
                    Column.physical("TABLE_CAT", DataTypes.STRING())
                            .withComment("Catalog name. NULL if not applicable."));

    /** Schema for {@link HiveServer2Endpoint#GetSchemas}. */
    public static final ResolvedSchema GET_SCHEMAS_SCHEMA =
            buildSchema(
                    Column.physical("TABLE_SCHEM", DataTypes.STRING())
                            .withComment("Schema name. NULL if not applicable."),
                    Column.physical("TABLE_CATALOG", DataTypes.STRING())
                            .withComment("Catalog name. NULL if not applicable"));

    /** Schema for {@link HiveServer2Endpoint#GetTables}. */
    public static final ResolvedSchema GET_TABLES_SCHEMA =
            buildSchema(
                    Column.physical("TABLE_CAT", DataTypes.STRING())
                            .withComment("Catalog name. NULL if not applicable."),
                    Column.physical("TABLE_SCHEM", DataTypes.STRING())
                            .withComment("Schema name. NULL if not applicable."),
                    Column.physical("TABLE_NAME", DataTypes.STRING())
                            .withComment("Table name. NULL if not applicable."),
                    Column.physical("TABLE_TYPE", DataTypes.STRING())
                            .withComment("The table type, e.g. \"TABLE\", \"VIEW\", etc."),
                    Column.physical("REMARKS", DataTypes.STRING())
                            .withComment("Comments about the table."),
                    Column.physical("TYPE_CAT", DataTypes.STRING())
                            .withComment("The types catalog."),
                    Column.physical("TYPE_SCHEM", DataTypes.STRING())
                            .withComment("The types schema."),
                    Column.physical("TYPE_NAME", DataTypes.STRING()).withComment("Type name."),
                    Column.physical("SELF_REFERENCING_COL_NAME", DataTypes.STRING())
                            .withComment(
                                    "Name of the designated \"identifier\" column of a typed table."),
                    Column.physical("REF_GENERATION", DataTypes.STRING())
                            .withComment(
                                    "Specifies how values in SELF_REFERENCING_COL_NAME are created."));

    /** Schema for {@link HiveServer2Endpoint#GetFunctions}. */
    public static final ResolvedSchema GET_FUNCTIONS_SCHEMA =
            buildSchema(
                    Column.physical("FUNCTION_CAT", DataTypes.STRING())
                            .withComment("Function catalog (may be null)"),
                    Column.physical("FUNCTION_SCHEM", DataTypes.STRING())
                            .withComment("Function schema (may be null)"),
                    Column.physical("FUNCTION_NAME", DataTypes.STRING())
                            .withComment(
                                    "Function name. This is the name used to invoke the function"),
                    Column.physical("REMARKS", DataTypes.STRING())
                            .withComment("Explanatory comment on the function"),
                    Column.physical("FUNCTION_TYPE", DataTypes.INT())
                            .withComment("Kind of function."),
                    Column.physical("SPECIFIC_NAME", DataTypes.STRING())
                            .withComment(
                                    "The name which uniquely identifies this function within its schema"));

    /** Schema for {@link HiveServer2Endpoint#GetColumns}. */
    public static final ResolvedSchema GET_COLUMNS_SCHEMA =
            buildSchema(
                    Column.physical("TABLE_CAT", DataTypes.STRING())
                            .withComment("Catalog name. NULL if not applicable."),
                    Column.physical("TABLE_SCHEM", DataTypes.STRING()).withComment("Schema name."),
                    Column.physical("TABLE_NAME", DataTypes.STRING()).withComment("Table name."),
                    Column.physical("COLUMN_NAME", DataTypes.STRING()).withComment("Column name."),
                    Column.physical("DATA_TYPE", DataTypes.INT())
                            .withComment("SQL type from java.sql.Types."),
                    Column.physical("TYPE_NAME", DataTypes.STRING())
                            .withComment(
                                    "Data source dependent type name, for a UDT the type name is fully qualified."),
                    Column.physical("COLUMN_SIZE", DataTypes.INT())
                            .withComment(
                                    "Column size. For char or date types this is the maximum number of characters, for numeric or decimal types this is precision."),
                    Column.physical("BUFFER_LENGTH", DataTypes.TINYINT()).withComment("Unused."),
                    Column.physical("DECIMAL_DIGITS", DataTypes.INT())
                            .withComment("The number of fractional digits."),
                    Column.physical("NUM_PREC_RADIX", DataTypes.INT())
                            .withComment("Radix (typically either 10 or 2)."),
                    Column.physical("NULLABLE", DataTypes.INT()).withComment("Is NULL allowed."),
                    Column.physical("REMARKS", DataTypes.STRING())
                            .withComment("Comment describing column (may be null)."),
                    Column.physical("COLUMN_DEF", DataTypes.STRING())
                            .withComment("Default value (may be null)."),
                    Column.physical("SQL_DATA_TYPE", DataTypes.INT()).withComment("Unused."),
                    Column.physical("SQL_DATETIME_SUB", DataTypes.INT()).withComment("Unused."),
                    Column.physical("CHAR_OCTET_LENGTH", DataTypes.INT())
                            .withComment(
                                    "For char types the maximum number of bytes in the column."),
                    Column.physical("ORDINAL_POSITION", DataTypes.INT())
                            .withComment("Index of column in table (starting at 1)."),
                    Column.physical("IS_NULLABLE", DataTypes.STRING())
                            .withComment(
                                    "\"NO\" means column definitely does not allow NULL values; \"YES\" means the column might allow NULL values. An empty string means nobody knows."),
                    Column.physical("SCOPE_CATALOG", DataTypes.STRING())
                            .withComment(
                                    "Catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)."),
                    Column.physical("SCOPE_SCHEMA", DataTypes.STRING())
                            .withComment(
                                    "Schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)."),
                    Column.physical("SCOPE_TABLE", DataTypes.STRING())
                            .withComment(
                                    "Table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)."),
                    Column.physical("SOURCE_DATA_TYPE", DataTypes.SMALLINT())
                            .withComment(
                                    "Source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)."),
                    Column.physical("IS_AUTO_INCREMENT", DataTypes.STRING())
                            .withComment("Indicates whether this column is auto incremented."));

    /** Schema for {@link HiveServer2Endpoint#GetTableTypes}. */
    public static final ResolvedSchema GET_TABLE_TYPES_SCHEMA =
            buildSchema(
                    Column.physical("TABLE_TYPE", DataTypes.STRING())
                            .withComment("Table type name."));

    /** Schema for {@link HiveServer2Endpoint#GetPrimaryKeys}. */
    public static final ResolvedSchema GET_PRIMARY_KEYS_SCHEMA =
            buildSchema(
                    Column.physical("TABLE_CAT", DataTypes.STRING())
                            .withComment("Table catalog (may be null)."),
                    Column.physical("TABLE_SCHEM", DataTypes.STRING())
                            .withComment("Table schema (may be null)."),
                    Column.physical("TABLE_NAME", DataTypes.STRING()).withComment("Table name."),
                    Column.physical("COLUMN_NAME", DataTypes.STRING()).withComment("Column name."),
                    Column.physical("KEY_SEQ", DataTypes.INT())
                            .withComment("Sequence number within primary key."),
                    Column.physical("PK_NAME", DataTypes.STRING())
                            .withComment("Primary key name (may be null)."));

    /** Schema for {@link HiveServer2Endpoint#GetTypeInfo}. */
    public static final ResolvedSchema GET_TYPE_INFO_SCHEMA =
            buildSchema(
                    Column.physical("TYPE_NAME", DataTypes.STRING()).withComment("Type name."),
                    Column.physical("DATA_TYPE", DataTypes.INT())
                            .withComment("SQL data type from java.sql.Types."),
                    Column.physical("PRECISION", DataTypes.INT()).withComment("Maximum precision."),
                    Column.physical("LITERAL_PREFIX", DataTypes.STRING())
                            .withComment("Prefix used to quote a literal (may be null)."),
                    Column.physical("LITERAL_SUFFIX", DataTypes.STRING())
                            .withComment("Suffix used to quote a literal (may be null)."),
                    Column.physical("CREATE_PARAMS", DataTypes.STRING())
                            .withComment("Parameters used in creating the type (may be null)."),
                    Column.physical("NULLABLE", DataTypes.SMALLINT())
                            .withComment("Can you use NULL for this type."),
                    Column.physical("CASE_SENSITIVE", DataTypes.BOOLEAN())
                            .withComment("Is it case sensitive."),
                    Column.physical("SEARCHABLE", DataTypes.SMALLINT())
                            .withComment("Can you use \"WHERE\" based on this type."),
                    Column.physical("UNSIGNED_ATTRIBUTE", DataTypes.BOOLEAN())
                            .withComment("Is it unsigned."),
                    Column.physical("FIXED_PREC_SCALE", DataTypes.BOOLEAN())
                            .withComment("Can it be a money value."),
                    Column.physical("AUTO_INCREMENT", DataTypes.BOOLEAN())
                            .withComment("Can it be used for an auto-increment value."),
                    Column.physical("LOCAL_TYPE_NAME", DataTypes.STRING())
                            .withComment("Localized version of type name (may be null)."),
                    Column.physical("MINIMUM_SCALE", DataTypes.SMALLINT())
                            .withComment("Minimum scale supported."),
                    Column.physical("MAXIMUM_SCALE", DataTypes.SMALLINT())
                            .withComment("Maximum scale supported."),
                    Column.physical("SQL_DATA_TYPE", DataTypes.INT()).withComment("Unused."),
                    Column.physical("SQL_DATETIME_SUB", DataTypes.INT()).withComment("Unused."),
                    Column.physical("NUM_PREC_RADIX", DataTypes.INT())
                            .withComment("Usually 2 or 10."));

    private static ResolvedSchema buildSchema(Column... columns) {
        return new ResolvedSchema(Arrays.asList(columns), Collections.emptyList(), null);
    }
}
