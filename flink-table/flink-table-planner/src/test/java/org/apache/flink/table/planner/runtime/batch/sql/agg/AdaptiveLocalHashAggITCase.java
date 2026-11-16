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

package org.apache.flink.table.planner.runtime.batch.sql.agg;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;

/** {@link HashAggITCase} with adaptive local hash aggregation enabled. */
class AdaptiveLocalHashAggITCase extends HashAggITCase {

    @Override
    public boolean adaptiveLocalHashAggEnable() {
        return true;
    }

    @Test
    void testAvgNullAfterAdaptiveBypass() {
        env().setParallelism(1);
        tEnv().getConfig()
                .set(
                        ExecutionConfigOptions
                                .TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD,
                        1L);
        tEnv().getConfig()
                .set(
                        ExecutionConfigOptions
                                .TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_DISTINCT_VALUE_RATE_THRESHOLD,
                        0.0);
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(1, 5, 5.0),
                                Row.of(1, null, null),
                                Row.of(2, null, null),
                                Row.of(2, 7, 7.0),
                                Row.of(3, null, null)));
        tEnv().executeSql(
                        "CREATE TABLE avg_input (g INT, x INT, d DOUBLE) "
                                + "WITH ('connector' = 'values', 'bounded' = 'true', "
                                + "'enable-aggregate-push-down' = 'false', 'data-id' = '"
                                + dataId
                                + "')");
        checkResult(
                "SELECT g, AVG(x), AVG(d), AVG(CAST(x AS DECIMAL(10, 2))), SUM(x), COUNT(x)"
                        + " FROM avg_input GROUP BY g",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(1, 5, 5.0, new BigDecimal("5.000000"), 5, 1L),
                                Row.of(2, 7, 7.0, new BigDecimal("7.000000"), 7, 1L),
                                Row.of(3, null, null, null, null, 0L))),
                false);
    }

    @Test
    void testAvgOverflowInNonEmptyPartialAggregate() {
        env().setParallelism(1);
        tEnv().getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_ENABLED, false);
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(1, "99999999999999999999999999999999999999"),
                                Row.of(1, "99999999999999999999999999999999999999")));
        tEnv().executeSql(
                        "CREATE TABLE overflow_input (g INT, x STRING) "
                                + "WITH ('connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                                + dataId
                                + "')");
        checkResult(
                "SELECT g, AVG(CAST(x AS DECIMAL(38, 0))), COUNT(x)"
                        + " FROM overflow_input GROUP BY g",
                JavaScalaConversionUtil.toScala(Arrays.asList(Row.of(1, null, 2L))),
                false);
    }
}
