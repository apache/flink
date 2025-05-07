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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for model table-valued function. */
public class ModelTableFunctionTest extends TableTestBase {

    private TableTestUtil util;

    @BeforeEach
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());

        // Create test table
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING,\n"
                                + "  d DECIMAL(10, 3),\n"
                                + "  rowtime TIMESTAMP(3),\n"
                                + "  proctime as PROCTIME(),\n"
                                + "  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND\n"
                                + ") with (\n"
                                + "  'connector' = 'values'\n"
                                + ")");

        // Create test model
        util.tableEnv()
                .executeSql(
                        "CREATE MODEL MyModel\n"
                                + "INPUT (a INT, b BIGINT)\n"
                                + "OUTPUT(c STRING, D ARRAY<INT>)\n"
                                + "with (\n"
                                + "  'provider' = 'openai'\n"
                                + ")");
    }

    @Test
    public void testMLPredictTVFWithNamedArguments() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(data => TABLE MyTable, "
                        + "input_model => MODEL MyModel, "
                        + "input_column => DESCRIPTOR(a, b)))";
        assertReachesRelConverter(sql);
    }

    @Test
    public void testMLPredictTVF() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b)))";
        assertReachesRelConverter(sql);
    }

    private void assertReachesRelConverter(String sql) {
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining("while converting MODEL");
    }
}
