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

package org.apache.flink.table.planner.operations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.ConvertTableToMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.FullAlterMaterializedTableOperation;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for in-place conversion of a regular table to a materialized table via {@code CREATE OR
 * ALTER MATERIALIZED TABLE}.
 */
class SqlNodeToOperationConvertTableToMaterializedTableTest
        extends SqlNodeToOperationConversionTestBase {

    private static final String SOURCE_REGULAR_TABLE_NAME = "src_table";

    @BeforeEach
    void before() throws TableAlreadyExistException, DatabaseNotExistException {
        super.before();
        sourceTable(SOURCE_REGULAR_TABLE_NAME).create();
        sourceTable("t1_with_ts").create();
    }

    @Nested
    class OperationSelection {
        private static final String EXISTING_MT_NAME = "existing_mt";

        @Test
        void missingTargetCreatesMaterializedTable() {
            final String sql =
                    "CREATE OR ALTER MATERIALIZED TABLE brand_new" + " AS SELECT a, b FROM t1";
            assertThat(parse(sql)).isInstanceOf(CreateMaterializedTableOperation.class);
        }

        @Test
        void existingMaterializedTableAlters()
                throws TableAlreadyExistException, DatabaseNotExistException {
            configureConversionEnabled(true);
            createExistingMaterializedTable();
            final String sql =
                    "CREATE OR ALTER MATERIALIZED TABLE "
                            + EXISTING_MT_NAME
                            + " AS SELECT a, b FROM t1";
            assertThat(parse(sql)).isInstanceOf(FullAlterMaterializedTableOperation.class);
        }

        @Test
        void regularTableWithConversionDisabledIsRejected() {
            configureConversionEnabled(false);
            final String sql =
                    "CREATE OR ALTER MATERIALIZED TABLE "
                            + SOURCE_REGULAR_TABLE_NAME
                            + " AS SELECT a, b FROM t1";
            assertThatThrownBy(() -> parse(sql))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            "Regular table does not support create or alter operation.");
        }

        @Test
        void regularTableWithConversionEnabledIsConverted() {
            configureConversionEnabled(true);
            final String sql =
                    "CREATE OR ALTER MATERIALIZED TABLE "
                            + SOURCE_REGULAR_TABLE_NAME
                            + " AS SELECT a, b FROM t1";
            assertThat(parse(sql)).isInstanceOf(ConvertTableToMaterializedTableOperation.class);
        }

        @Test
        void viewIsRejected() throws TableAlreadyExistException, DatabaseNotExistException {
            // A view is rejected regardless of the conversion flag: only tables convert.
            configureConversionEnabled(true);
            final CatalogView view =
                    CatalogView.of(
                            Schema.newBuilder()
                                    .column("a", DataTypes.BIGINT())
                                    .column("b", DataTypes.STRING())
                                    .build(),
                            null,
                            "SELECT a, b FROM t1",
                            "SELECT a, b FROM t1",
                            Map.of());
            catalog.createTable(
                    new ObjectPath(catalogManager.getCurrentDatabase(), "src_view"), view, false);

            assertThatThrownBy(
                            () ->
                                    parse(
                                            "CREATE OR ALTER MATERIALIZED TABLE src_view"
                                                    + " AS SELECT a, b FROM t1"))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            "VIEW does not support the CREATE OR ALTER MATERIALIZED TABLE operation.");
        }

        private void createExistingMaterializedTable()
                throws TableAlreadyExistException, DatabaseNotExistException {
            final String sql =
                    "CREATE MATERIALIZED TABLE existing_mt (\n"
                            + "  CONSTRAINT pk1 PRIMARY KEY(a) NOT ENFORCED\n"
                            + ")\n"
                            + "AS SELECT a, b FROM t1";
            final Operation op = parse(sql);
            assertThat(op).isInstanceOf(CreateMaterializedTableOperation.class);
            final CatalogMaterializedTable mt =
                    ((CreateMaterializedTableOperation) op).getCatalogMaterializedTable();
            catalog.createTable(
                    new ObjectPath(catalogManager.getCurrentDatabase(), EXISTING_MT_NAME),
                    mt,
                    true);
        }
    }

    @Nested
    class ConfigScope {

        @Test
        void sessionOnlyEnableHasNoEffect() {
            tableConfig.set(
                    TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED, true);
            // root configuration left default (false)

            assertThatThrownBy(() -> parse(conversionSql()))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            "Regular table does not support create or alter operation.");
        }

        @Test
        void clusterRootEnableAllowsConversion() {
            configureConversionEnabled(true);

            assertThat(parse(conversionSql()))
                    .isInstanceOf(ConvertTableToMaterializedTableOperation.class);
        }

        @Test
        void bothSessionAndClusterEnabledAllowsConversion() {
            tableConfig.set(
                    TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED, true);
            configureConversionEnabled(true);

            assertThat(parse(conversionSql()))
                    .isInstanceOf(ConvertTableToMaterializedTableOperation.class);
        }

        @Test
        void neitherSessionNorClusterEnabledIsRejected() {
            // nothing set
            assertThatThrownBy(() -> parse(conversionSql()))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            "Regular table does not support create or alter operation.");
        }

        private String conversionSql() {
            return "CREATE OR ALTER MATERIALIZED TABLE "
                    + SOURCE_REGULAR_TABLE_NAME
                    + " AS SELECT a, b FROM t1";
        }
    }

    @Nested
    class WatermarkAndPrimaryKey {

        @BeforeEach
        void enableConversion() {
            configureConversionEnabled(true);
        }

        @Test
        void sourceWatermarkAndPrimaryKeyAreNotInherited()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_wm_pk").withWatermark().withPrimaryKey().create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_wm_pk"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "ts");
            assertThat(newSchema.getWatermarkSpecs()).isEmpty();
            assertThat(newSchema.getPrimaryKey()).isEmpty();
        }

        @Test
        void columnListWithoutWatermarkOrPrimaryKeyDropsThem()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_list").withWatermark().withPrimaryKey().create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_list ("
                                    + " a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3))"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "ts");
            assertThat(newSchema.getWatermarkSpecs()).isEmpty();
            assertThat(newSchema.getPrimaryKey()).isEmpty();
        }

        @Test
        void declaredWatermarkAndPrimaryKeyArePresent()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_neither").create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_neither ("
                                    + " a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3),"
                                    + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,"
                                    + " PRIMARY KEY (a) NOT ENFORCED)"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "ts");
            assertThat(newSchema.getWatermarkSpecs()).isNotEmpty();
            assertThat(newSchema.getPrimaryKey()).isPresent();
        }

        @Test
        void watermarkAndPrimaryKeyAreKeptWhenDeclared()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_keep_wm_pk").withPrimaryKey().withWatermark().create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_keep_wm_pk ("
                                    + " PRIMARY KEY (a) NOT ENFORCED,"
                                    + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "ts");
            assertThat(newSchema.getPrimaryKey()).isPresent();
            assertThat(newSchema.getWatermarkSpecs()).isNotEmpty();
        }

        @Test
        void declaredWatermarkOnlyDropsSourcePrimaryKey()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_pk_only").withPrimaryKey().create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_pk_only ("
                                    + " a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3),"
                                    + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "ts");
            assertThat(newSchema.getWatermarkSpecs()).isNotEmpty();
            assertThat(newSchema.getPrimaryKey()).isEmpty();
        }

        @Test
        void declaredPrimaryKeyOnlyDropsSourceWatermark()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_wm_only").withWatermark().create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_wm_only ("
                                    + " a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3),"
                                    + " PRIMARY KEY (a) NOT ENFORCED)"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "ts");
            assertThat(newSchema.getWatermarkSpecs()).isEmpty();
            assertThat(newSchema.getPrimaryKey()).isPresent();
        }

        @Test
        void neitherSourceNorStatementDeclaresThem()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_plain").create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_plain"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "ts");
            assertThat(newSchema.getWatermarkSpecs()).isEmpty();
            assertThat(newSchema.getPrimaryKey()).isEmpty();
        }

        private ResolvedSchema convertedMaterializedTableSchema(String sql) {
            return convertedOperation(sql).getMaterializedTable().getResolvedSchema();
        }
    }

    @Nested
    class SchemaDerivation {

        @BeforeEach
        void enableConversion() {
            configureConversionEnabled(true);
        }

        @Test
        void declaredPhysicalColumnMissingFromQueryIsRejected()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_missing_col").create();

            assertThatThrownBy(
                            () ->
                                    parse(
                                            "CREATE OR ALTER MATERIALIZED TABLE src_missing_col ("
                                                    + " a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3), extra STRING)"
                                                    + " AS SELECT a, b, ts FROM t1_with_ts"))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("could not be found in the query");
        }

        @Test
        void declaredCompatibleTypeGetsImplicitCast()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_implicit_cast").create();

            // Query produces INT for `a`; the declared BIGINT is kept and an implicit cast is
            // added.
            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_implicit_cast ("
                                    + " a BIGINT, b STRING, ts TIMESTAMP(3))"
                                    + " AS SELECT CAST(a AS INT) AS a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "ts");
            assertThat(newSchema.getColumn("a"))
                    .hasValueSatisfying(
                            column ->
                                    assertThat(column.getDataType().getLogicalType().getTypeRoot())
                                            .isSameAs(LogicalTypeRoot.BIGINT));
        }

        @Test
        void declaredIncompatibleTypeIsRejected()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_incompatible").create();

            assertThatThrownBy(
                            () ->
                                    parse(
                                            "CREATE OR ALTER MATERIALIZED TABLE src_incompatible ("
                                                    + " a BIGINT NOT NULL, b INT, ts TIMESTAMP(3))"
                                                    + " AS SELECT a, b, ts FROM t1_with_ts"))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("Incompatible types for sink column 'b'");
        }

        @Test
        void computedAndVirtualMetadataColumnsAreAllowedAndNotPersisted()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_virtual").create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_virtual ("
                                    + " a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3),"
                                    + " c AS current_timestamp,"
                                    + " m STRING METADATA VIRTUAL)"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getColumnNames()).containsExactly("c", "m", "a", "b", "ts");
            assertThat(newSchema.getColumn("c"))
                    .hasValueSatisfying(
                            column -> {
                                assertThat(column).isInstanceOf(Column.ComputedColumn.class);
                                assertThat(column.isPersisted()).isFalse();
                            });
            assertThat(newSchema.getColumn("m"))
                    .hasValueSatisfying(
                            column -> {
                                assertThat(column).isInstanceOf(Column.MetadataColumn.class);
                                assertThat(column.isPersisted()).isFalse();
                            });
        }

        @Test
        void columnIdentifiersOnlyReorderQueryColumns()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_reorder").create();

            final ResolvedSchema newSchema =
                    convertedMaterializedTableSchema(
                            "CREATE OR ALTER MATERIALIZED TABLE src_reorder (b, a, ts)"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            assertThat(newSchema.getColumnNames()).containsExactly("b", "a", "ts");
        }

        @Test
        void columnIdentifiersOnlyCountMismatchIsRejected()
                throws TableAlreadyExistException, DatabaseNotExistException {
            sourceTable("src_reorder_bad").create();

            assertThatThrownBy(
                            () ->
                                    parse(
                                            "CREATE OR ALTER MATERIALIZED TABLE src_reorder_bad (a, b)"
                                                    + " AS SELECT a, b, ts FROM t1_with_ts"))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("number of columns in the column list must match");
        }

        @Test
        void nonPersistedSourceColumnsAreNotInConvertedSchema()
                throws TableAlreadyExistException, DatabaseNotExistException {
            // Source has physical columns plus a computed column `c` and a virtual metadata
            // column `m`, neither produced by the query.
            sourceTable("src_non_persisted").withNonPersistedColumns().create();

            final ConvertTableToMaterializedTableOperation op =
                    convertedOperation(
                            "CREATE OR ALTER MATERIALIZED TABLE src_non_persisted"
                                    + " AS SELECT a, b, ts FROM t1_with_ts");

            // The converted materialized table is built from the query, so the non-persisted
            // source columns are not part of its schema. This is the schema the catalog persists.
            assertThat(op.getMaterializedTable().getResolvedSchema().getColumnNames())
                    .containsExactly("a", "b", "ts");

            // Without a DDL column list the change list keeps non-persisted columns: no drop is
            // emitted for `c` or `m`.
            assertThat(op.getTableChanges())
                    .noneMatch(
                            change ->
                                    change instanceof TableChange.DropColumn
                                            && List.of("c", "m")
                                                    .contains(
                                                            ((TableChange.DropColumn) change)
                                                                    .getColumnName()));
        }

        private ResolvedSchema convertedMaterializedTableSchema(String sql) {
            return convertedOperation(sql).getMaterializedTable().getResolvedSchema();
        }
    }

    @Nested
    class QueryEvolution {

        @BeforeEach
        void enableConversion() {
            configureConversionEnabled(true);
        }

        @Test
        void queryAddsColumnNotInSourceAddsItToNewMaterializedTable()
                throws TableAlreadyExistException, DatabaseNotExistException {
            // src has columns a, b, ts
            sourceTable("src_add_col").create();

            final ConvertTableToMaterializedTableOperation op =
                    convertedOperation(
                            "CREATE OR ALTER MATERIALIZED TABLE src_add_col"
                                    + " AS SELECT a, b, ts, CAST('extra' AS STRING) AS c FROM t1_with_ts");

            final ResolvedSchema newSchema = op.getMaterializedTable().getResolvedSchema();
            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "ts", "c");
            assertThat(op.getTableChanges())
                    .anyMatch(
                            change ->
                                    change instanceof TableChange.AddColumn
                                            && "c"
                                                    .equals(
                                                            ((TableChange.AddColumn) change)
                                                                    .getColumn()
                                                                    .getName()));
        }

        @Test
        void queryDropsColumnFromSourceDropsItFromNewMaterializedTable()
                throws TableAlreadyExistException, DatabaseNotExistException {
            // src has columns a, b, ts
            sourceTable("src_drop_col").create();

            final ConvertTableToMaterializedTableOperation op =
                    convertedOperation(
                            "CREATE OR ALTER MATERIALIZED TABLE src_drop_col"
                                    + " AS SELECT a, b FROM t1_with_ts");

            final ResolvedSchema newSchema = op.getMaterializedTable().getResolvedSchema();
            assertThat(newSchema.getColumnNames()).containsExactly("a", "b");
            assertThat(op.getTableChanges())
                    .anyMatch(
                            change ->
                                    change instanceof TableChange.DropColumn
                                            && "ts"
                                                    .equals(
                                                            ((TableChange.DropColumn) change)
                                                                    .getColumnName()));
        }

        @Test
        void queryRenamesColumnViaAliasIsModeledAsDropAndAdd()
                throws TableAlreadyExistException, DatabaseNotExistException {
            // src has columns a, b, ts
            sourceTable("src_rename_col").create();

            final ConvertTableToMaterializedTableOperation op =
                    convertedOperation(
                            "CREATE OR ALTER MATERIALIZED TABLE src_rename_col"
                                    + " AS SELECT a, b, ts AS event_time FROM t1_with_ts");

            final ResolvedSchema newSchema = op.getMaterializedTable().getResolvedSchema();
            assertThat(newSchema.getColumnNames()).containsExactly("a", "b", "event_time");
            assertThat(op.getTableChanges())
                    .anyMatch(
                            change ->
                                    change instanceof TableChange.DropColumn
                                            && "ts"
                                                    .equals(
                                                            ((TableChange.DropColumn) change)
                                                                    .getColumnName()))
                    .anyMatch(
                            change ->
                                    change instanceof TableChange.AddColumn
                                            && "event_time"
                                                    .equals(
                                                            ((TableChange.AddColumn) change)
                                                                    .getColumn()
                                                                    .getName()));
        }

        @Test
        void queryChangesColumnTypeIsModeledAsModifyPhysicalColumnType()
                throws TableAlreadyExistException, DatabaseNotExistException {
            // src has columns a BIGINT NOT NULL, b STRING, ts TIMESTAMP(3)
            sourceTable("src_type_change").create();

            final ConvertTableToMaterializedTableOperation op =
                    convertedOperation(
                            "CREATE OR ALTER MATERIALIZED TABLE src_type_change"
                                    + " AS SELECT CAST(a AS INT) AS a, b, ts FROM t1_with_ts");

            final ResolvedSchema newSchema = op.getMaterializedTable().getResolvedSchema();
            assertThat(newSchema.getColumn("a"))
                    .hasValueSatisfying(
                            column ->
                                    assertThat(column.getDataType().getLogicalType().getTypeRoot())
                                            .isSameAs(LogicalTypeRoot.INTEGER));
            assertThat(op.getTableChanges())
                    .anyMatch(change -> change instanceof TableChange.ModifyPhysicalColumnType);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private void configureConversionEnabled(boolean enabled) {
        final Configuration root = new Configuration();
        root.set(TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED, enabled);
        tableConfig.setRootConfiguration(root);
    }

    private ConvertTableToMaterializedTableOperation convertedOperation(String sql) {
        final Operation op = parse(sql);
        assertThat(op).isInstanceOf(ConvertTableToMaterializedTableOperation.class);
        return (ConvertTableToMaterializedTableOperation) op;
    }

    private SourceTableBuilder sourceTable(String name) {
        return new SourceTableBuilder(name);
    }

    /** Fluent builder for registering a regular {@code (a, b, ts)} source table in the catalog. */
    private final class SourceTableBuilder {

        private final String name;
        private boolean withWatermark;
        private boolean withPrimaryKey;
        private boolean withNonPersistedColumns;

        private SourceTableBuilder(String name) {
            this.name = name;
        }

        SourceTableBuilder withWatermark() {
            this.withWatermark = true;
            return this;
        }

        SourceTableBuilder withPrimaryKey() {
            this.withPrimaryKey = true;
            return this;
        }

        SourceTableBuilder withNonPersistedColumns() {
            this.withNonPersistedColumns = true;
            return this;
        }

        void create() throws TableAlreadyExistException, DatabaseNotExistException {
            final Schema.Builder schema = Schema.newBuilder();
            schema.column("a", DataTypes.BIGINT().notNull());
            schema.column("b", DataTypes.STRING());
            schema.column("ts", DataTypes.TIMESTAMP(3));
            if (withNonPersistedColumns) {
                schema.columnByExpression("c", "current_timestamp");
                schema.columnByMetadata("m", DataTypes.STRING(), null, true);
            }
            if (withWatermark) {
                schema.watermark("ts", new SqlCallExpression("ts - INTERVAL '5' SECOND"));
            }
            if (withPrimaryKey) {
                schema.primaryKeyNamed("pk_src", List.of("a"));
            }
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "COLLECTION");
            final CatalogTable table =
                    CatalogTable.newBuilder().schema(schema.build()).options(options).build();
            catalog.createTable(
                    new ObjectPath(catalogManager.getCurrentDatabase(), name), table, false);
        }
    }
}
