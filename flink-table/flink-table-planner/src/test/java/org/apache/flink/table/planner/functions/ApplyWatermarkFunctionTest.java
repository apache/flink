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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link org.apache.flink.table.planner.functions.sql.SqlApplyWatermarkFunction}.
 *
 * <p>NOTE: These tests are disabled until the APPLY_WATERMARK function is fully wired as a
 * SqlTableFunction with proper column-reference scoping for the watermark expression argument. The
 * SQL validator currently resolves the third argument (watermark expression) in the outer scope, so
 * column references to the input table (e.g. {@code event_time}) cannot be resolved. Tracking:
 * FLINK-39062 follow-up.
 */
public class ApplyWatermarkFunctionTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @Test
    @Disabled("FLINK-39062: APPLY_WATERMARK TVF scope wiring pending")
    public void testApplyWatermarkFunctionBasic() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source_table (\n"
                                + "  id BIGINT,\n"
                                + "  event_time TIMESTAMP(3),\n"
                                + "  val STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'datagen',\n"
                                + "  'number-of-rows' = '10'\n"
                                + ")");

        String sql =
                "SELECT * FROM APPLY_WATERMARK(\n"
                        + "  TABLE source_table,\n"
                        + "  DESCRIPTOR(event_time),\n"
                        + "  event_time - INTERVAL '5' SECOND\n"
                        + ")";

        util.verifyRelPlan(sql);
    }

    @Test
    @Disabled("FLINK-39062: APPLY_WATERMARK TVF scope wiring pending")
    public void testApplyWatermarkWithSubquery() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source_table (\n"
                                + "  id BIGINT,\n"
                                + "  event_time TIMESTAMP(3),\n"
                                + "  val STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'datagen',\n"
                                + "  'number-of-rows' = '10'\n"
                                + ")");

        String sql =
                "SELECT * FROM APPLY_WATERMARK(\n"
                        + "  TABLE (SELECT * FROM source_table WHERE id > 0),\n"
                        + "  DESCRIPTOR(event_time),\n"
                        + "  event_time - INTERVAL '10' SECOND\n"
                        + ")";

        util.verifyRelPlan(sql);
    }

    @Test
    @Disabled("FLINK-39062: APPLY_WATERMARK TVF scope wiring pending")
    public void testApplyWatermarkWithTimestampLTZ() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source_table (\n"
                                + "  id BIGINT,\n"
                                + "  event_time TIMESTAMP_LTZ(3),\n"
                                + "  val STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'datagen',\n"
                                + "  'number-of-rows' = '10'\n"
                                + ")");

        String sql =
                "SELECT * FROM APPLY_WATERMARK(\n"
                        + "  TABLE source_table,\n"
                        + "  DESCRIPTOR(event_time),\n"
                        + "  event_time\n"
                        + ")";

        util.verifyRelPlan(sql);
    }
}
