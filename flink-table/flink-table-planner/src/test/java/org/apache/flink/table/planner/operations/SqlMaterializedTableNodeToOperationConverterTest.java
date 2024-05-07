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
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the materialized table statements for {@link SqlNodeToOperationConversion}. */
public class SqlMaterializedTableNodeToOperationConverterTest
        extends SqlNodeToOperationConversionTestBase {

    @Test
    public void testCreateMaterializedTable() {
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1 (\n"
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
        assertThat(operation).isInstanceOf(CreateMaterializedTableOperation.class);

        CreateMaterializedTableOperation op = (CreateMaterializedTableOperation) operation;
        CatalogMaterializedTable materializedTable = op.getCatalogMaterializedTable();
        assertThat(materializedTable).isInstanceOf(ResolvedCatalogMaterializedTable.class);

        Map<String, String> options = new HashMap<>();
        options.put("connector", "filesystem");
        options.put("format", "json");
        CatalogMaterializedTable expected =
                CatalogMaterializedTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.BIGINT().notNull())
                                        .column("b", DataTypes.VARCHAR(Integer.MAX_VALUE))
                                        .column("c", DataTypes.INT())
                                        .column("d", DataTypes.VARCHAR(Integer.MAX_VALUE))
                                        .primaryKeyNamed("ct1", Collections.singletonList("a"))
                                        .build())
                        .comment("materialized table comment")
                        .options(options)
                        .partitionKeys(Arrays.asList("a", "d"))
                        .freshness(Duration.ofSeconds(30))
                        .logicalRefreshMode(CatalogMaterializedTable.LogicalRefreshMode.FULL)
                        .refreshMode(CatalogMaterializedTable.RefreshMode.FULL)
                        .refreshStatus(CatalogMaterializedTable.RefreshStatus.INITIALIZING)
                        .definitionQuery(
                                "SELECT `t1`.`a`, `t1`.`b`, `t1`.`c`, `t1`.`d`\n"
                                        + "FROM `builtin`.`default`.`t1` AS `t1`")
                        .build();

        assertThat(((ResolvedCatalogMaterializedTable) materializedTable).getOrigin())
                .isEqualTo(expected);
    }

    @Test
    public void testContinuousRefreshMode() {
        // test continuous mode derived by specify freshness automatically
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "AS SELECT * FROM t1";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(CreateMaterializedTableOperation.class);

        CreateMaterializedTableOperation op = (CreateMaterializedTableOperation) operation;
        CatalogMaterializedTable materializedTable = op.getCatalogMaterializedTable();
        assertThat(materializedTable).isInstanceOf(ResolvedCatalogMaterializedTable.class);

        assertThat(materializedTable.getLogicalRefreshMode())
                .isEqualTo(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC);
        assertThat(materializedTable.getRefreshMode())
                .isEqualTo(CatalogMaterializedTable.RefreshMode.CONTINUOUS);

        // test continuous mode by manual specify
        final String sql2 =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL '30' DAY\n"
                        + "REFRESH_MODE = CONTINUOUS\n"
                        + "AS SELECT * FROM t1";
        Operation operation2 = parse(sql2);
        assertThat(operation2).isInstanceOf(CreateMaterializedTableOperation.class);

        CreateMaterializedTableOperation op2 = (CreateMaterializedTableOperation) operation2;
        CatalogMaterializedTable materializedTable2 = op2.getCatalogMaterializedTable();
        assertThat(materializedTable2).isInstanceOf(ResolvedCatalogMaterializedTable.class);

        assertThat(materializedTable2.getLogicalRefreshMode())
                .isEqualTo(CatalogMaterializedTable.LogicalRefreshMode.CONTINUOUS);
        assertThat(materializedTable2.getRefreshMode())
                .isEqualTo(CatalogMaterializedTable.RefreshMode.CONTINUOUS);
    }

    @Test
    public void testFullRefreshMode() {
        // test full mode derived by specify freshness automatically
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL '1' DAY\n"
                        + "AS SELECT * FROM t1";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(CreateMaterializedTableOperation.class);

        CreateMaterializedTableOperation op = (CreateMaterializedTableOperation) operation;
        CatalogMaterializedTable materializedTable = op.getCatalogMaterializedTable();
        assertThat(materializedTable).isInstanceOf(ResolvedCatalogMaterializedTable.class);

        assertThat(materializedTable.getLogicalRefreshMode())
                .isEqualTo(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC);
        assertThat(materializedTable.getRefreshMode())
                .isEqualTo(CatalogMaterializedTable.RefreshMode.FULL);

        // test full mode by manual specify
        final String sql2 =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        Operation operation2 = parse(sql2);
        assertThat(operation2).isInstanceOf(CreateMaterializedTableOperation.class);

        CreateMaterializedTableOperation op2 = (CreateMaterializedTableOperation) operation2;
        CatalogMaterializedTable materializedTable2 = op2.getCatalogMaterializedTable();
        assertThat(materializedTable2).isInstanceOf(ResolvedCatalogMaterializedTable.class);

        assertThat(materializedTable2.getLogicalRefreshMode())
                .isEqualTo(CatalogMaterializedTable.LogicalRefreshMode.FULL);
        assertThat(materializedTable2.getRefreshMode())
                .isEqualTo(CatalogMaterializedTable.RefreshMode.FULL);
    }

    @Test
    public void testCreateMaterializedTableWithInvalidPrimaryKey() {
        // test unsupported constraint
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1 (\n"
                        + "   CONSTRAINT ct1 UNIQUE(a) NOT ENFORCED"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "AS SELECT * FROM t1";

        assertThatThrownBy(() -> parse(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Primary key validation failed: UNIQUE constraint is not supported yet.");

        // test primary key not defined in source table
        final String sql2 =
                "CREATE MATERIALIZED TABLE mtbl1 (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(e) NOT ENFORCED"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "AS SELECT * FROM t1";

        assertThatThrownBy(() -> parse(sql2))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Primary key column 'e' not defined in the query schema. Available columns: ['a', 'b', 'c', 'd'].");

        // test primary key with nullable source column
        final String sql3 =
                "CREATE MATERIALIZED TABLE mtbl1 (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(d) NOT ENFORCED"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "AS SELECT * FROM t1";

        assertThatThrownBy(() -> parse(sql3))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Could not create a PRIMARY KEY with nullable column 'd'.");
    }

    @Test
    public void testCreateMaterializedTableWithInvalidPartitionKey() {
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "PARTITIONED BY (a, e)\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        assertThatThrownBy(() -> parse(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Partition column 'e' not defined in the query schema. Available columns: ['a', 'b', 'c', 'd'].");
    }

    @Test
    public void testCreateMaterializedTableWithInvalidFreshnessType() {
        // test negative freshness value
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL -'30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        assertThatThrownBy(() -> parse(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Materialized table freshness doesn't support negative value.");

        // test unsupported freshness type
        final String sql2 =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL '30' YEAR\n"
                        + "AS SELECT * FROM t1";
        assertThatThrownBy(() -> parse(sql2))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Materialized table freshness only support SECOND, MINUTE, HOUR, DAY as the time unit.");

        // test unsupported freshness type
        final String sql3 =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL '30' DAY TO HOUR\n"
                        + "AS SELECT * FROM t1";
        assertThatThrownBy(() -> parse(sql3))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Materialized table freshness only support SECOND, MINUTE, HOUR, DAY as the time unit.");
    }
}
