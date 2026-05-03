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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Interval;
import org.apache.flink.table.catalog.Interval.TimeUnit;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.StartMode.StartModeKind;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.FullAlterMaterializedTableOperation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlNodeToOperationSqlCreateOrAlterMaterializedTableConverterTest
        extends SqlNodeToOperationConversionTestBase {
    private static final String DEFAULT_MATERIALIZED_TABLE =
            "CREATE MATERIALIZED TABLE mt (\n"
                    + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                    + ")\n"
                    + "COMMENT 'materialized table comment'\n"
                    + "PARTITIONED BY (a, d)\n"
                    + "WITH (\n"
                    + "  'connector' = 'filesystem', \n"
                    + "  'format' = 'json'\n"
                    + ")\n"
                    + "FRESHNESS = INTERVAL '30' SECOND\n"
                    + "REFRESH_MODE = FULL\n"
                    + "AS SELECT * FROM t1";

    @BeforeEach
    void before() throws TableAlreadyExistException, DatabaseNotExistException {
        super.before();
        createMaterializedTableInCatalog(DEFAULT_MATERIALIZED_TABLE, "mt");
    }

    @Test
    void testAlterMaterializedTableAsQueryWithDefinedSchema() {
        String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt ("
                        + "`a` BIGINT NOT NULL, `b` STRING, `c` INT, `d` STRING, `a1` BIGINT NOT NULL, `f` INT) "
                        + "AS SELECT a, b, c, d, a as `a1`, 3 as f FROM t1";
        FullAlterMaterializedTableOperation sqlAlterMaterializedTableAsQuery =
                (FullAlterMaterializedTableOperation) parse(sql);

        assertThat(sqlAlterMaterializedTableAsQuery.getTableChanges())
                .containsExactly(
                        // If NOT NULL is defined in schema, it should stay
                        TableChange.add(Column.physical("a1", DataTypes.BIGINT().notNull())),
                        TableChange.add(Column.physical("f", DataTypes.INT())),
                        TableChange.dropConstraint("ct1"),
                        TableChange.modifyDefinitionQuery(
                                "SELECT `a`, `b`, `c`, `d`, `a` AS `a1`, 3 AS `f`\nFROM `t1`",
                                "SELECT `t1`.`a`, `t1`.`b`, `t1`.`c`, `t1`.`d`, `t1`.`a` AS `a1`, 3 AS `f`\n"
                                        + "FROM `builtin`.`default`.`t1` AS `t1`"),
                        TableChange.reset("connector"),
                        TableChange.reset("format"));
    }

    @Test
    void testAlterMaterializedTableAsQueryWithoutDefinedSchema() {
        String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt "
                        + "AS SELECT a, b, c, d, a as `a1` FROM t1";
        FullAlterMaterializedTableOperation sqlAlterMaterializedTableAsQuery =
                (FullAlterMaterializedTableOperation) parse(sql);

        assertThat(sqlAlterMaterializedTableAsQuery.getTableChanges())
                .containsExactly(
                        // No explicit schema, so nullable will be used
                        TableChange.add(Column.physical("a1", DataTypes.BIGINT())),
                        TableChange.modifyDefinitionQuery(
                                "SELECT `a`, `b`, `c`, `d`, `a` AS `a1`\nFROM `t1`",
                                "SELECT `t1`.`a`, `t1`.`b`, `t1`.`c`, `t1`.`d`, `t1`.`a` AS `a1`\n"
                                        + "FROM `builtin`.`default`.`t1` AS `t1`"),
                        TableChange.reset("connector"),
                        TableChange.reset("format"));
    }

    @ParameterizedTest
    @MethodSource("createOrAlterForExistingMaterializedTableFailedCaseSpecs")
    void createOrAlterForExistingMaterializedTableFailedCase(TestSpec spec) {
        Operation operation = parse(spec.sql);
        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        // Will be invoked while operation#execute
        assertThatThrownBy(op::getTableChanges)
                .isInstanceOf(spec.expectedException)
                .hasMessage(spec.errMessage);
    }

    private static Collection<TestSpec> createOrAlterForExistingMaterializedTableFailedCaseSpecs() {
        return List.of(
                TestSpec.of(
                        "CREATE OR ALTER MATERIALIZED TABLE mt (\n"
                                + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                                + ")\n"
                                + "REFRESH_MODE = CONTINUOUS\n"
                                + "AS SELECT * FROM t1",
                        "Changing of REFRESH MODE is unsupported"),
                TestSpec.of(
                        "CREATE OR ALTER MATERIALIZED TABLE mt (\n"
                                + "   a BIGINT, b INT, c INT, d INT, "
                                + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                                + ")\n"
                                + "AS SELECT * FROM t1",
                        "Incompatible types for sink column 'b' at position 2. "
                                + "The source column has type 'STRING', while the target column has type 'INT'."));
    }

    @Test
    void testCreateOrAlterMaterializedTableForExistingTableNoChanges() {
        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                        + ")\n"
                        + "COMMENT 'materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        assertThat(op.getTableChanges()).isEmpty();
        assertThat(operation.asSummaryString())
                .isEqualTo("CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt\n");
    }

    @Test
    void testCreateOrAlterMaterializedTableForExistingTable() throws TableNotExistException {
        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                        + ")\n"
                        + "COMMENT 'materialized table comment'\n"
                        + "DISTRIBUTED BY HASH (b) INTO 7 BUCKETS\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'format' = 'json2'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT a, b, c, d, d as e, cast('123' as string) as f FROM t1";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        assertThat(op.getTableChanges())
                .containsExactly(
                        TableChange.add(Column.physical("e", DataTypes.VARCHAR(Integer.MAX_VALUE))),
                        TableChange.add(Column.physical("f", DataTypes.VARCHAR(Integer.MAX_VALUE))),
                        TableChange.modifyDefinitionQuery(
                                "SELECT `a`, `b`, `c`, `d`, `d` AS `e`, CAST('123' AS STRING) AS `f`\nFROM `t1`",
                                "SELECT `t1`.`a`, `t1`.`b`, `t1`.`c`, `t1`.`d`, `t1`.`d` AS `e`, CAST('123' AS STRING) AS `f`\n"
                                        + "FROM `builtin`.`default`.`t1` AS `t1`"),
                        TableChange.set("format", "json2"),
                        TableChange.reset("connector"),
                        TableChange.add(
                                TableDistribution.of(
                                        TableDistribution.Kind.HASH, 7, List.of("b"))));
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt\n"
                                + "  ADD `e` STRING ,\n"
                                + "  ADD `f` STRING ,\n"
                                + "  MODIFY DEFINITION QUERY TO 'SELECT `t1`.`a`, `t1`.`b`, `t1`.`c`, `t1`.`d`, `t1`.`d` AS `e`, CAST('123' AS STRING) AS `f`\n"
                                + "FROM `builtin`.`default`.`t1` AS `t1`',\n"
                                + "  SET 'format' = 'json2',\n"
                                + "  RESET 'connector',\n"
                                + "  ADD DISTRIBUTED BY HASH(`b`) INTO 7 BUCKETS");

        // new table only difference schema & definition query with old table.
        CatalogMaterializedTable oldTable =
                (CatalogMaterializedTable)
                        catalog.getTable(new ObjectPath(catalogManager.getCurrentDatabase(), "mt"));
        CatalogMaterializedTable newTable = op.getNewTable();

        assertThat(newTable.getOptions()).containsExactly(Map.entry("format", "json2"));
        assertThat(oldTable.getUnresolvedSchema()).isNotEqualTo(newTable.getUnresolvedSchema());
        assertThat(oldTable.getUnresolvedSchema().getPrimaryKey())
                .isEqualTo(newTable.getUnresolvedSchema().getPrimaryKey());
        assertThat(oldTable.getUnresolvedSchema().getWatermarkSpecs())
                .isEqualTo(newTable.getUnresolvedSchema().getWatermarkSpecs());
        assertThat(oldTable.getOriginalQuery()).isNotEqualTo(newTable.getOriginalQuery());
        assertThat(oldTable.getExpandedQuery()).isNotEqualTo(newTable.getExpandedQuery());
        assertThat(oldTable.getDefinitionFreshness()).isEqualTo(newTable.getDefinitionFreshness());
        assertThat(oldTable.getRefreshMode()).isEqualTo(newTable.getRefreshMode());
        assertThat(oldTable.getRefreshStatus()).isEqualTo(newTable.getRefreshStatus());
        assertThat(oldTable.getSerializedRefreshHandler())
                .isEqualTo(newTable.getSerializedRefreshHandler());

        List<Schema.UnresolvedColumn> addedColumn =
                newTable.getUnresolvedSchema().getColumns().stream()
                        .filter(
                                column ->
                                        !oldTable.getUnresolvedSchema()
                                                .getColumns()
                                                .contains(column))
                        .collect(Collectors.toList());
        // added column should be a nullable column.
        assertThat(addedColumn)
                .containsExactly(
                        new Schema.UnresolvedPhysicalColumn(
                                "e", DataTypes.VARCHAR(Integer.MAX_VALUE)),
                        new Schema.UnresolvedPhysicalColumn(
                                "f", DataTypes.VARCHAR(Integer.MAX_VALUE)));
    }

    @Test
    void testCreateOrAlterMaterializedTableWithDistributionForExistingTable()
            throws TableAlreadyExistException, DatabaseNotExistException {
        final String prepSql =
                "CREATE MATERIALIZED TABLE mt2\n"
                        + "DISTRIBUTED BY HASH (a) INTO 5 BUCKETS\n"
                        + "AS SELECT t1.* FROM t1";
        createMaterializedTableInCatalog(prepSql, "mt2");

        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt2\n"
                        + "DISTRIBUTED BY HASH (b) INTO 4 BUCKETS\n"
                        + "AS SELECT t1.* FROM t1";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        assertThat(op.getTableChanges())
                .containsExactly(
                        TableChange.modify(
                                TableDistribution.of(
                                        TableDistribution.Kind.HASH, 4, List.of("b"))));
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt2\n"
                                + "  MODIFY DISTRIBUTED BY HASH(`b`) INTO 4 BUCKETS");
    }

    @Test
    void testCreateOrAlterMaterializedTableWithDroppedConstraint() {
        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt (a BIGINT NOT NULL, b STRING, c INT, d STRING)\n"
                        + "COMMENT 'New materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        assertThat(op.getTableChanges()).containsExactly(TableChange.dropConstraint("ct1"));
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt\n"
                                + "  DROP CONSTRAINT ct1");
    }

    @Test
    void testCreateOrAlterMaterializedTableWithoutSchemaConstraintShouldStay() {
        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt \n"
                        + "COMMENT 'New materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        assertThat(op.getTableChanges()).isEmpty();
        assertThat(operation.asSummaryString())
                .isEqualTo("CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt\n");
    }

    @Test
    void testCreateOrAlterMaterializedTableWithChangedConstraint() {
        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt (\n"
                        + "   CONSTRAINT new_constraint PRIMARY KEY(a) NOT ENFORCED"
                        + ")\n"
                        + "COMMENT 'New materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        assertThat(op.getTableChanges())
                .containsExactly(
                        TableChange.modify(
                                UniqueConstraint.primaryKey("new_constraint", List.of("a"))));
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt\n"
                                + "  MODIFY CONSTRAINT `new_constraint` PRIMARY KEY (`a`) NOT ENFORCED");
    }

    @Test
    void testCreateOrAlterMaterializedTableWithNewConstraint()
            throws TableAlreadyExistException, DatabaseNotExistException {
        final String prepSql =
                "CREATE MATERIALIZED TABLE mt1 (\n"
                        + "   id INT NOT NULL, t TIMESTAMP_LTZ(3)\n"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        createMaterializedTableInCatalog(prepSql, "mt1");

        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt1 (\n"
                        + "   id INT NOT NULL, t TIMESTAMP_LTZ(3),\n"
                        + "   CONSTRAINT new_constraint PRIMARY KEY(id) NOT ENFORCED"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        assertThat(op.getTableChanges())
                .containsExactly(
                        TableChange.add(
                                UniqueConstraint.primaryKey("new_constraint", List.of("id"))));
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt1\n"
                                + "  ADD CONSTRAINT `new_constraint` PRIMARY KEY (`id`) NOT ENFORCED");
    }

    @Test
    void testCreateOrAlterMaterializedTableWithAddedWatermark()
            throws TableAlreadyExistException, DatabaseNotExistException {
        final String prepSql =
                "CREATE MATERIALIZED TABLE mt1 (\n"
                        + "   id INT, t TIMESTAMP_LTZ(3)"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        createMaterializedTableInCatalog(prepSql, "mt1");

        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt1 (\n"
                        + "   id INT, t TIMESTAMP_LTZ(3),\n"
                        + "   WATERMARK FOR t as current_timestamp - INTERVAL '5' SECOND"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt1\n"
                                + "  ADD WATERMARK FOR `t`: TIMESTAMP_LTZ(3) NOT NULL AS CURRENT_TIMESTAMP - INTERVAL '5' SECOND");
    }

    @Test
    void testCreateOrAlterMaterializedTableWithModifiedWatermark()
            throws TableAlreadyExistException, DatabaseNotExistException {
        final String prepSql =
                "CREATE MATERIALIZED TABLE mt1 (\n"
                        + "   id INT, t TIMESTAMP_LTZ(3),\n"
                        + "   WATERMARK FOR t as current_timestamp - INTERVAL '5' SECOND\n"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        createMaterializedTableInCatalog(prepSql, "mt1");

        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt1 (\n"
                        + "   id INT, t TIMESTAMP_LTZ(3),\n"
                        + "   WATERMARK FOR t as current_timestamp - INTERVAL '12' HOUR\n"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt1\n"
                                + "  MODIFY WATERMARK FOR `t`: TIMESTAMP_LTZ(3) NOT NULL AS CURRENT_TIMESTAMP - INTERVAL '12' HOUR");
    }

    @Test
    void testCreateOrAlterMaterializedTableWithDroppedWatermark()
            throws TableAlreadyExistException, DatabaseNotExistException {
        final String prepSql =
                "CREATE MATERIALIZED TABLE mt1 (\n"
                        + "   id INT, t TIMESTAMP_LTZ(3),\n"
                        + "   WATERMARK FOR t as current_timestamp - INTERVAL '5' SECOND"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        createMaterializedTableInCatalog(prepSql, "mt1");

        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt1 (\n"
                        + "   id INT, t TIMESTAMP_LTZ(3)"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt1\n"
                                + "  DROP WATERMARK");
    }

    @Test
    void testCreateOrAlterMaterializedTableWithoutSchemaWatermarkShouldStay()
            throws TableAlreadyExistException, DatabaseNotExistException {
        final String prepSql =
                "CREATE MATERIALIZED TABLE mt1 (\n"
                        + "   id INT, t TIMESTAMP_LTZ(3),\n"
                        + "   WATERMARK FOR t as current_timestamp - INTERVAL '5' SECOND"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        createMaterializedTableInCatalog(prepSql, "mt1");

        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt1\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        assertThat(operation.asSummaryString())
                .isEqualTo("CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt1\n");
    }

    @Test
    void testCreateOrAlterMaterializedTableWithCommentChange()
            throws TableAlreadyExistException, DatabaseNotExistException {
        final String prepSql =
                "CREATE MATERIALIZED TABLE mt1 (\n"
                        + "   id INT COMMENT 'INT comment', t TIMESTAMP_LTZ(3)\n"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        createMaterializedTableInCatalog(prepSql, "mt1");

        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt1 (\n"
                        + "   id INT, t TIMESTAMP_LTZ(3) COMMENT 'Timestamp Comment'\n"
                        + ")\n"
                        + "AS SELECT 1 as id, current_timestamp as t";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        assertThat(op.getTableChanges())
                .containsExactly(
                        TableChange.modifyColumnComment(
                                Column.physical("id", DataTypes.INT()).withComment("INT comment"),
                                null),
                        TableChange.modifyColumnComment(
                                Column.physical("t", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                                "Timestamp Comment"));
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "CREATE OR ALTER MATERIALIZED TABLE builtin.default.mt1\n"
                                + "  MODIFY `id` COMMENT '',\n"
                                + "  MODIFY `t` COMMENT 'Timestamp Comment'");
    }

    @ParameterizedTest
    @EnumSource(
            value = StartModeKind.class,
            names = {
                "FROM_NOW",
                "RESUME_OR_FROM_NOW",
                "RESUME_OR_FROM_BEGINNING",
                "FROM_BEGINNING"
            })
    void testCreateOrAlterMaterializedTableWithNotChangedStartMode(StartModeKind kind)
            throws TableAlreadyExistException, DatabaseNotExistException {
        final String prepSql =
                String.format(
                        "CREATE MATERIALIZED TABLE mt1\n"
                                + "START_MODE=%s\n"
                                + "AS SELECT * FROM t1",
                        kind.toString());
        createMaterializedTableInCatalog(prepSql, "mt1");

        final String sql =
                String.format(
                        "CREATE OR ALTER MATERIALIZED TABLE mt1\n"
                                + "START_MODE=%s\n"
                                + "AS SELECT * FROM t1",
                        kind);
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        assertThat(op.getTableChanges()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(
            value = StartModeKind.class,
            names = {
                "FROM_NOW",
                "RESUME_OR_FROM_NOW",
                "RESUME_OR_FROM_BEGINNING",
                "FROM_BEGINNING"
            })
    void testCreateOrAlterMaterializedTableWithFinalDefaultStartMode(StartModeKind kind)
            throws TableAlreadyExistException, DatabaseNotExistException {
        final String prepSql =
                String.format(
                        "CREATE MATERIALIZED TABLE mt1\n"
                                + "START_MODE=%s\n"
                                + "AS SELECT * FROM t1",
                        kind.toString());
        createMaterializedTableInCatalog(prepSql, "mt1");

        final String sql = "CREATE OR ALTER MATERIALIZED TABLE mt1\n" + "AS SELECT * FROM t1";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        if (kind == StartModeKind.FROM_BEGINNING) {
            assertThat(op.getTableChanges()).isEmpty();
        } else {
            assertThat(op.getTableChanges())
                    .contains(
                            TableChange.modifyStartMode(
                                    StartMode.of(StartModeKind.FROM_BEGINNING)));
        }
    }

    @ParameterizedTest
    @MethodSource("startModeAlterCases")
    void testCreateOrAlterMaterializedTableWithChangedStartMode(
            String newStartModeClause, StartMode newStartMode)
            throws TableAlreadyExistException, DatabaseNotExistException {
        final String prepSql =
                "CREATE MATERIALIZED TABLE mt1\n"
                        + "START_MODE=FROM_BEGINNING\n"
                        + "AS SELECT * FROM t1";
        createMaterializedTableInCatalog(prepSql, "mt1");

        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mt1\n"
                        + "START_MODE = "
                        + newStartModeClause
                        + "\n"
                        + "AS SELECT * FROM t1";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(FullAlterMaterializedTableOperation.class);

        FullAlterMaterializedTableOperation op = (FullAlterMaterializedTableOperation) operation;
        assertThat(op.getTableChanges()).containsExactly(TableChange.modifyStartMode(newStartMode));
    }

    private static Collection<Arguments> startModeAlterCases() {
        return List.of(
                Arguments.of(
                        "RESUME_OR_FROM_BEGINNING",
                        StartMode.of(StartModeKind.RESUME_OR_FROM_BEGINNING)),
                Arguments.of("FROM_NOW", StartMode.of(StartModeKind.FROM_NOW)),
                Arguments.of(
                        "FROM_NOW(INTERVAL '2' HOUR)",
                        StartMode.of(StartModeKind.FROM_NOW, Interval.of(2, TimeUnit.HOUR))),
                Arguments.of(
                        "FROM_NOW",
                        StartMode.of(StartModeKind.FROM_NOW),
                        "FROM_TIMESTAMP(TIMESTAMP '1234-12-10 11:22:33')",
                        StartMode.of(
                                StartModeKind.FROM_TIMESTAMP,
                                Instant.parse("1234-12-10T11:22:33Z"))),
                Arguments.of("RESUME_OR_FROM_NOW", StartMode.of(StartModeKind.RESUME_OR_FROM_NOW)),
                Arguments.of(
                        "RESUME_OR_FROM_TIMESTAMP(TIMESTAMP '2025-01-15 10:00:00')",
                        StartMode.of(
                                StartModeKind.RESUME_OR_FROM_TIMESTAMP,
                                Instant.parse("2025-01-15T10:00:00Z"))));
    }

    private void createMaterializedTableInCatalog(String sql, String materializedTableName)
            throws TableAlreadyExistException, DatabaseNotExistException {
        final ObjectPath objectPath =
                new ObjectPath(catalogManager.getCurrentDatabase(), materializedTableName);
        final CreateMaterializedTableOperation operation = createMaterializedTableOperation(sql);
        catalog.createTable(objectPath, operation.getCatalogMaterializedTable(), true);
    }

    private CreateMaterializedTableOperation createMaterializedTableOperation(String sql) {
        final Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(CreateMaterializedTableOperation.class);
        return (CreateMaterializedTableOperation) operation;
    }

    private static class TestSpec {
        private final String sql;
        private final Class<?> expectedException;
        private final String errMessage;
        private final String expectedSchema;

        private TestSpec(String sql, Class<?> expectedException, String errMessage) {
            this.sql = sql;
            this.expectedException = expectedException;
            this.errMessage = errMessage;
            this.expectedSchema = null;
        }

        private TestSpec(String sql, String expectedSchema) {
            this.sql = sql;
            this.expectedException = null;
            this.errMessage = null;
            this.expectedSchema = expectedSchema;
        }

        public static TestSpec of(String sql, Class<?> expectedException, String errMessage) {
            return new TestSpec(sql, expectedException, errMessage);
        }

        public static TestSpec of(String sql, String errMessage) {
            return of(sql, ValidationException.class, errMessage);
        }

        public static TestSpec withExpectedSchema(String sql, String expectedSchema) {
            return new TestSpec(sql, expectedSchema);
        }

        @Override
        public String toString() {
            return sql;
        }
    }
}
