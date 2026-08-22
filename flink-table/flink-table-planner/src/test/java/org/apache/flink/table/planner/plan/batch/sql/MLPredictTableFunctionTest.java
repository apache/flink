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

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.common.MLPredictTableFunctionTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for ML_PREDICT table function in batch mode. */
public class MLPredictTableFunctionTest extends MLPredictTableFunctionTestBase {

    @Override
    protected TableTestUtil getUtil() {
        return batchTestUtil(TableConfig.getDefault());
    }

    @Test
    public void testIllegalConfig() {
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT *\n"
                                                + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b), MAP['async', true]))"))
                .isInstanceOf(ValidationException.class)
                .hasRootCauseMessage(
                        "No match found for function signature ML_PREDICT(<RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, DECIMAL(10, 3) d)>, <RecordType(VARCHAR(2147483647) e, INTEGER ARRAY f)>, <COLUMN_LIST>, <(CHAR(5), BOOLEAN) MAP>).\n"
                                + "Supported signatures are:\n"
                                + "ML_PREDICT(INPUT => {TABLE, ROW SEMANTIC TABLE}, MODEL => {MODEL}, ARGS => DESCRIPTOR, CONFIG => MAP<STRING, STRING>)");

        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT *\n"
                                                + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b), MAP['async', 'yes']))"))
                .hasCauseInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Failed to parse the config.");

        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT *\n"
                                                + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b), MAP['async', 'true', 'max-concurrent-operations', '-1']))"))
                .hasCauseInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Invalid runtime config option 'max-concurrent-operations'. Its value should be positive integer but was -1.");

        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT *\n"
                                                + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b), MAP['async', 'true', 'capacity', CAST(-1 AS STRING)]))"))
                .hasCauseInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Config parameter should be a MAP data type consisting of String literals.");

        assertThatThrownBy(
                        () ->
                                util.verifyExecPlan(
                                        "SELECT *\n"
                                                + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b), MAP['async', 'true']))"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Require async mode, but model provider org.apache.flink.table.factories.TestModelProviderFactory$TestModelProviderMock doesn't support async mode.");
    }
}
