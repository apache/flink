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

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.common.MLPredictTableFunctionTestBase;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for model table value function in stream mode. */
public class MLPredictTableFunctionTest extends MLPredictTableFunctionTestBase {

    @Override
    protected TableTestUtil getUtil() {
        return streamTestUtil(TableConfig.getDefault());
    }

    @Override
    @BeforeEach
    public void setup() {
        util = getUtil();

        // Create test table with stream-specific columns
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
                                + "OUTPUT(e STRING, f ARRAY<INT>)\n"
                                + "with (\n"
                                + "  'provider' = 'test-model',\n"
                                + "  'endpoint' = 'someendpoint',\n"
                                + "  'task' = 'text_generation'\n"
                                + ")");
    }

    @Test
    public void testInputTableIsInsertOnlyStream() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b)))";
        util.verifyRelPlan(
                sql,
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.CHANGELOG_MODE)));
    }

    @Test
    public void testInputTableIsCdcStream() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE CdcTable(\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  PRIMARY KEY (a) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'changelog-mode' = 'I,UA,UB,D'"
                                + ")");
        String sql =
                "SELECT *\n" + "FROM ML_PREDICT(TABLE CdcTable, MODEL MyModel, DESCRIPTOR(a, b))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "StreamPhysicalMLPredictTableFunction doesn't support consuming update and delete changes which is produced by node TableSourceScan(table=[[default_catalog, default_database, CdcTable]], fields=[a, b])");
    }
}
