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

import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ReplaceTableAsOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.types.AbstractDataType;

import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test base for testing convert [CREATE OR] REPLACE TABLE AS statement to operation. */
public class SqlRTASNodeToOperationConverterTest extends SqlNodeToOperationConversionTestBase {

    @Test
    public void testReplaceTableAS() {
        String sql = "REPLACE TABLE t_t WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";
        ObjectIdentifier expectedIdentifier = ObjectIdentifier.of("builtin", "default", "t_t");
        Operation operation = parseAndConvert(sql);
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("k1", "v1");
        expectedOptions.put("k2", "v2");
        Schema expectedSchema =
                Schema.newBuilder()
                        .fromFields(
                                new String[] {"a", "b", "c", "d"},
                                new AbstractDataType[] {
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING(),
                                    DataTypes.INT(),
                                    DataTypes.STRING()
                                })
                        .build();
        CatalogTable expectedCatalogTable =
                CatalogTable.of(expectedSchema, null, Collections.emptyList(), expectedOptions);
        verifyReplaceTableAsOperation(operation, expectedIdentifier, expectedCatalogTable);
    }

    @Test
    public void testReplaceTableAsThrowException() {
        // should throw exception for specifying column
        final String sql1 =
                "REPLACE TABLE t_t (b_1 string, c_1 bigint METADATA, c_1 as 1 + 1)"
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT b, c FROM t1";
        assertThatThrownBy(() -> parseAndConvert(sql1))
                .isInstanceOf(SqlValidateException.class)
                .hasMessage(
                        "REPLACE TABLE AS SELECT syntax does not support to specify explicit columns yet.");

        // should throw exception for specifying watermark
        final String sql2 =
                "REPLACE TABLE t_t (WATERMARK FOR eventTime AS eventTime - INTERVAL '5' SECOND)"
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT b, c FROM t1";
        assertThatThrownBy(() -> parseAndConvert(sql2))
                .isInstanceOf(SqlValidateException.class)
                .hasMessage(
                        "REPLACE TABLE AS SELECT syntax does not support to specify explicit watermark yet.");

        // should throw exception for tmp table
        final String sql3 = "REPLACE TEMPORARY TABLE t WITH ('test' = 'zm') AS SELECT b, c FROM t1";
        assertThatThrownBy(() -> parseAndConvert(sql3))
                .isInstanceOf(SqlValidateException.class)
                .hasMessage("REPLACE TABLE AS SELECT syntax does not support temporary table yet.");

        // should throw exception for partition key
        final String sql4 =
                "REPLACE TABLE t PARTITIONED BY(b) WITH ('test' = 'zm') AS SELECT b FROM t1";
        assertThatThrownBy(() -> parseAndConvert(sql4))
                .isInstanceOf(SqlValidateException.class)
                .hasMessage(
                        "REPLACE TABLE AS SELECT syntax does not support to create partitioned table yet.");
    }

    @Test
    public void testCreateOrReplaceTableAS() {
        String sql =
                "CREATE OR REPLACE TABLE t_t WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";
        ObjectIdentifier expectedIdentifier = ObjectIdentifier.of("builtin", "default", "t_t");
        Operation operation = parseAndConvert(sql);
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("k1", "v1");
        expectedOptions.put("k2", "v2");
        Schema expectedSchema =
                Schema.newBuilder()
                        .fromFields(
                                new String[] {"a", "b", "c", "d"},
                                new AbstractDataType[] {
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING(),
                                    DataTypes.INT(),
                                    DataTypes.STRING()
                                })
                        .build();
        CatalogTable expectedCatalogTable =
                CatalogTable.of(expectedSchema, null, Collections.emptyList(), expectedOptions);
        verifyReplaceTableAsOperation(operation, expectedIdentifier, expectedCatalogTable);
    }

    @Test
    public void testCreateOrReplaceTableAsThrowException() {
        // should throw exception for specifying column
        final String sql1 =
                "CREATE OR REPLACE TABLE t_t (b_1 string, c_1 bigint METADATA, c_1 as 1 + 1)"
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT b, c FROM t1";
        assertThatThrownBy(() -> parseAndConvert(sql1))
                .isInstanceOf(SqlValidateException.class)
                .hasMessage(
                        "CREATE OR REPLACE TABLE AS SELECT syntax does not support to specify explicit columns yet.");

        // should throw exception for specifying watermark
        final String sql2 =
                "CREATE OR REPLACE TABLE t_t (WATERMARK FOR eventTime AS eventTime - INTERVAL '5' SECOND)"
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT b, c FROM t1";
        assertThatThrownBy(() -> parseAndConvert(sql2))
                .isInstanceOf(SqlValidateException.class)
                .hasMessage(
                        "CREATE OR REPLACE TABLE AS SELECT syntax does not support to specify explicit watermark yet.");

        // should throw exception for tmp table
        final String sql3 =
                "CREATE OR REPLACE TEMPORARY TABLE t WITH ('test' = 'zm') AS SELECT b, c FROM t1";
        assertThatThrownBy(() -> parseAndConvert(sql3))
                .isInstanceOf(SqlValidateException.class)
                .hasMessage(
                        "CREATE OR REPLACE TABLE AS SELECT syntax does not support temporary table yet.");

        // should throw exception for partition key
        final String sql4 =
                "CREATE OR REPLACE TABLE t PARTITIONED BY(b) WITH ('test' = 'zm') AS SELECT b FROM t1";
        assertThatThrownBy(() -> parseAndConvert(sql4))
                .isInstanceOf(SqlValidateException.class)
                .hasMessage(
                        "CREATE OR REPLACE TABLE AS SELECT syntax does not support to create partitioned table yet.");
    }

    private Operation parseAndConvert(String sql) {
        final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);

        SqlNode node = parser.parse(sql);
        return SqlNodeToOperationConversion.convert(planner, catalogManager, node).get();
    }

    private void verifyReplaceTableAsOperation(
            Operation operation,
            ObjectIdentifier expectedTableIdentifier,
            CatalogTable expectedCatalogTable) {
        assertThat(operation).isInstanceOf(ReplaceTableAsOperation.class);
        ReplaceTableAsOperation replaceTableAsOperation = (ReplaceTableAsOperation) operation;
        CreateTableOperation createTableOperation =
                replaceTableAsOperation.getCreateTableOperation();
        // verify the createTableOperation
        assertThat(createTableOperation.isTemporary()).isFalse();
        assertThat(createTableOperation.isIgnoreIfExists()).isFalse();
        assertThat(createTableOperation.getTableIdentifier()).isEqualTo(expectedTableIdentifier);
        // verify the catalog table to be created
        verifyCatalogTable(expectedCatalogTable, createTableOperation.getCatalogTable());
    }

    private void verifyCatalogTable(
            CatalogTable expectedCatalogTable, CatalogTable actualCatalogTable) {
        assertThat(actualCatalogTable.getUnresolvedSchema())
                .isEqualTo(expectedCatalogTable.getUnresolvedSchema());
        assertThat(actualCatalogTable.getComment()).isEqualTo(expectedCatalogTable.getComment());
        assertThat(actualCatalogTable.getPartitionKeys())
                .isEqualTo(expectedCatalogTable.getPartitionKeys());
        assertThat(actualCatalogTable.getOptions()).isEqualTo(expectedCatalogTable.getOptions());
    }
}
