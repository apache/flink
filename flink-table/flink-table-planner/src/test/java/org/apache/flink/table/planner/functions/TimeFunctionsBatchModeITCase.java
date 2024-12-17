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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.utils.InternalConfigOptions;

import java.time.Instant;
import java.time.ZoneId;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.Expressions.currentDate;
import static org.apache.flink.table.api.Expressions.currentTime;
import static org.apache.flink.table.api.Expressions.currentTimestamp;
import static org.apache.flink.table.api.Expressions.localTime;
import static org.apache.flink.table.api.Expressions.localTimestamp;

/** Test time-related built-in functions. */
class TimeFunctionsBatchModeITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(timeRelatedDynamicFunctions()).flatMap(s -> s);
    }

    private Stream<TestSetSpec> timeRelatedDynamicFunctions() {
        TableConfig tableConfig = TableConfig.getDefault();

        tableConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        tableConfig.setLocalTimeZone(zoneId);
        tableConfig.set(InternalConfigOptions.TABLE_QUERY_START_EPOCH_TIME, 1123L);

        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.CURRENT_TIMESTAMP)
                        .withConfiguration(tableConfig)
                        .testResult(
                                localTime(),
                                "LOCALTIME",
                                Instant.ofEpochMilli(1123).atZone(zoneId).toLocalTime(),
                                TIME(0).notNull())
                        .testResult(
                                currentTime(),
                                "CURRENT_TIME",
                                Instant.ofEpochMilli(1123).atZone(zoneId).toLocalTime(),
                                TIME(0).notNull())
                        .testResult(
                                localTimestamp(),
                                "LOCALTIMESTAMP",
                                Instant.ofEpochMilli(1123).atZone(zoneId).toLocalDateTime(),
                                TIMESTAMP(3).notNull())
                        .testResult(
                                currentTimestamp(),
                                "CURRENT_TIMESTAMP",
                                Instant.ofEpochMilli(1123),
                                TIMESTAMP_LTZ(3).notNull())
                        .testResult(
                                currentDate(),
                                "CURRENT_DATE",
                                Instant.ofEpochMilli(1123).atZone(zoneId).toLocalDate(),
                                DATE().notNull()));
    }
}
