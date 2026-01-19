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

import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for window TVFs (TUMBLE, HOP, SESSION, CUMULATE) combined with ORDER BY clauses
 * in VIEW definitions.
 *
 * <p>These tests verify that views containing window table functions with ORDER BY can be created
 * and queried successfully. This ensures proper SQL generation and parsing when views are expanded
 * during query execution.
 *
 * <p>Test coverage includes:
 *
 * <ul>
 *   <li>TUMBLE window with ORDER BY
 *   <li>HOP window with ORDER BY
 *   <li>SESSION window with ORDER BY
 *   <li>CUMULATE window with ORDER BY
 * </ul>
 */
class WindowTVFWithOrderByITCase extends StreamingTestBase {

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();

        // Create a test table with watermark
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
    }

    @Test
    void testTumbleWindowWithOrderBy() {
        // Test TUMBLE window TVF with ORDER BY in VIEW
        tEnv().executeSql(
                        "CREATE TEMPORARY VIEW test_tumble AS "
                                + "SELECT * FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) "
                                + "ORDER BY ts");

        String explanation = tEnv().explainSql("SELECT * FROM test_tumble");
        assertThat(explanation).contains("WindowTableFunction");
    }

    @Test
    void testHopWindowWithOrderBy() {
        // Test HOP window TVF with ORDER BY in VIEW
        tEnv().executeSql(
                        "CREATE TEMPORARY VIEW hop_view AS "
                                + "SELECT * FROM TABLE(HOP(TABLE orders, DESCRIPTOR(ts), INTERVAL '30' SECOND, INTERVAL '1' MINUTE)) "
                                + "ORDER BY ts");

        String explanation = tEnv().explainSql("SELECT * FROM hop_view");
        assertThat(explanation).contains("WindowTableFunction");
    }

    @Test
    void testSessionWindowWithOrderBy() {
        // Test SESSION window TVF with ORDER BY in VIEW
        tEnv().executeSql(
                        "CREATE TEMPORARY VIEW session_view AS "
                                + "SELECT * FROM TABLE(SESSION(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) "
                                + "ORDER BY ts");

        String explanation = tEnv().explainSql("SELECT * FROM session_view");
        assertThat(explanation).contains("WindowTableFunction");
    }

    @Test
    void testCumulateWindowWithOrderBy() {
        // Test CUMULATE window TVF with ORDER BY in VIEW
        tEnv().executeSql(
                        "CREATE TEMPORARY VIEW cumulate_view AS "
                                + "SELECT * FROM TABLE(CUMULATE(TABLE orders, DESCRIPTOR(ts), INTERVAL '30' SECOND, INTERVAL '1' MINUTE)) "
                                + "ORDER BY ts");

        String explanation = tEnv().explainSql("SELECT * FROM cumulate_view");
        assertThat(explanation).contains("WindowTableFunction");
    }
}
