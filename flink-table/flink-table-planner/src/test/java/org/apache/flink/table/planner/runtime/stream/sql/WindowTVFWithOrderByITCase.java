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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for window TVFs combined with ORDER BY clauses in VIEW definitions.
 *
 * <p>These tests verify that views containing window table functions with ORDER BY can be created
 * and queried successfully. This ensures proper SQL generation and parsing when views are expanded
 * during query execution.
 */
class WindowTVFWithOrderByITCase extends StreamingTestBase {

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();

        tEnv().executeSql(
                        "CREATE TABLE orders ("
                                + "  orderId INT,"
                                + "  price DECIMAL(10, 2),"
                                + "  quantity INT,"
                                + "  ts TIMESTAMP_LTZ(3),"
                                + "  WATERMARK FOR ts AS ts"
                                + ") WITH ("
                                + "  'connector' = 'datagen',"
                                + "  'number-of-rows' = '10'"
                                + ")");
        tEnv().executeSql(
                        "CREATE TEMPORARY VIEW test_tumble AS "
                                + "SELECT * FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) "
                                + "ORDER BY ts");
    }

    @Test
    void testWindowTVFQueryWithOrderBy() {
        Table table = tEnv().sqlQuery("SELECT * FROM test_tumble");
        assertThat(table.getResolvedSchema().getColumnNames())
                .contains("window_start", "window_end", "window_time");
    }

    @Test
    void testWindowTVFExplainWithOrderBy() {
        String explanation = tEnv().explainSql("SELECT * FROM test_tumble");
        assertThat(explanation).contains("WindowTableFunction");
    }
}
