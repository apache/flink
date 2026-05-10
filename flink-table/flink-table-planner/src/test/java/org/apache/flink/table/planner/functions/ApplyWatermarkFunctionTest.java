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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link org.apache.flink.table.planner.functions.sql.SqlApplyWatermarkFunction} and the
 * accompanying {@link org.apache.flink.table.planner.plan.rules.logical.LogicalApplyWatermarkRule}.
 *
 * <p>The tests cover three concerns: (1) operand validation enforced at SQL-level by the function
 * metadata, (2) end-to-end planning that produces a {@code WatermarkAssigner} node, and (3)
 * preservation of base-table watermark semantics (override behaviour).
 */
public class ApplyWatermarkFunctionTest extends TableTestBase {

    private StreamTableTestUtil util;

    @BeforeEach
    public void setUp() {
        util = streamTestUtil(TableConfig.getDefault());
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
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source_table_ltz (\n"
                                + "  id BIGINT,\n"
                                + "  event_time TIMESTAMP_LTZ(3),\n"
                                + "  val STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'datagen',\n"
                                + "  'number-of-rows' = '10'\n"
                                + ")");
    }

    /**
     * APPLY_WATERMARK over a base table with a constant watermark expression. The watermark
     * expression here does not reference table columns and therefore can already be resolved by
     * the SQL validator without dedicated scope wiring (see FLINK-39062 follow-up for column
     * references in the watermark expression).
     */
    @Test
    public void testApplyWatermarkConstantExpression() {
        final String sql =
                "SELECT * FROM TABLE(APPLY_WATERMARK(\n"
                        + "  TABLE source_table,\n"
                        + "  DESCRIPTOR(event_time),\n"
                        + "  TIMESTAMP '1970-01-01 00:00:00.000'\n"
                        + "))";
        final String plan = util.tableEnv().explainSql(sql);
        // The compiled plan should contain a watermark assigner referencing event_time.
        assertThat(plan).contains("WatermarkAssigner");
        assertThat(plan).contains("rowtime=[event_time]");
    }

    @Test
    public void testApplyWatermarkOnTimestampLtz() {
        final String sql =
                "SELECT * FROM TABLE(APPLY_WATERMARK(\n"
                        + "  TABLE source_table_ltz,\n"
                        + "  DESCRIPTOR(event_time),\n"
                        + "  TO_TIMESTAMP_LTZ(0, 3)\n"
                        + "))";
        final String plan = util.tableEnv().explainSql(sql);
        assertThat(plan).contains("WatermarkAssigner");
    }

    /** DESCRIPTOR column must exist in the input. */
    @Test
    public void testApplyWatermarkWithUnknownColumnFails() {
        final String sql =
                "SELECT * FROM TABLE(APPLY_WATERMARK(\n"
                        + "  TABLE source_table,\n"
                        + "  DESCRIPTOR(missing_column),\n"
                        + "  TIMESTAMP '1970-01-01 00:00:00.000'\n"
                        + "))";
        assertThatThrownBy(() -> util.tableEnv().explainSql(sql))
                .hasMessageContaining("missing_column");
    }

    /** DESCRIPTOR column must be of TIMESTAMP / TIMESTAMP_LTZ type. */
    @Test
    public void testApplyWatermarkWithNonTimestampColumnFails() {
        final String sql =
                "SELECT * FROM TABLE(APPLY_WATERMARK(\n"
                        + "  TABLE source_table,\n"
                        + "  DESCRIPTOR(id),\n"
                        + "  TIMESTAMP '1970-01-01 00:00:00.000'\n"
                        + "))";
        assertThatThrownBy(() -> util.tableEnv().explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("TIMESTAMP");
    }

    /** Watermark expression must evaluate to TIMESTAMP / TIMESTAMP_LTZ. */
    @Test
    public void testApplyWatermarkWithNonTimestampExpressionFails() {
        final String sql =
                "SELECT * FROM TABLE(APPLY_WATERMARK(\n"
                        + "  TABLE source_table,\n"
                        + "  DESCRIPTOR(event_time),\n"
                        + "  CAST(1 AS BIGINT)\n"
                        + "))";
        assertThatThrownBy(() -> util.tableEnv().explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("TIMESTAMP");
    }

    /** Verify APPLY_WATERMARK is rejected when DESCRIPTOR carries multiple columns. */
    @Test
    public void testApplyWatermarkWithMultipleDescriptorColumnsFails() {
        final String sql =
                "SELECT * FROM TABLE(APPLY_WATERMARK(\n"
                        + "  TABLE source_table,\n"
                        + "  DESCRIPTOR(event_time, id),\n"
                        + "  TIMESTAMP '1970-01-01 00:00:00.000'\n"
                        + "))";
        assertThatThrownBy(() -> util.tableEnv().explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("exactly one column");
    }
}
