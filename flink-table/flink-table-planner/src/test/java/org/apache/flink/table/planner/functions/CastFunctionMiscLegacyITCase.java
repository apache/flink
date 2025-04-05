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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.functions.CastFunctionMiscITCase.LocalDateTimeToRaw;

import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_LEGACY_CAST_BEHAVIOUR;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER;
import static org.apache.flink.table.planner.functions.CastFunctionMiscITCase.serializeLocalDateTime;

/**
 * Tests for {@link BuiltInFunctionDefinitions#CAST} when legacy cast mode enabled regarding {@link
 * DataTypes#ROW}.
 */
class CastFunctionMiscLegacyITCase extends BuiltInFunctionTestBase {
    Configuration getConfiguration() {
        return super.getConfiguration()
                .set(
                        TABLE_EXEC_LEGACY_CAST_BEHAVIOUR,
                        ExecutionConfigOptions.LegacyCastBehaviour.ENABLED)
                .set(
                        TABLE_EXEC_SINK_NOT_NULL_ENFORCER,
                        ExecutionConfigOptions.NotNullEnforcer.ERROR);
    }

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST, "legacy cast failure returns null")
                        .onFieldsWithData("invalid")
                        .andDataTypes(STRING().notNull())
                        .testSqlRuntimeError(
                                "CAST(f0 AS BIGINT)",
                                "Column 'EXPR$0' is NOT NULL, however, a null value is "
                                        + "being written into it. You can set job configuration "
                                        + "'table.exec.sink.not-null-enforcer'='DROP' to suppress "
                                        + "this exception and drop such records silently."),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST,
                                "cast from RAW(LocalDateTime) to BINARY(13)")
                        .onFieldsWithData("2020-11-11T18:08:01.123")
                        .andDataTypes(STRING())
                        .withFunction(LocalDateTimeToRaw.class)
                        .testTableApiResult(
                                call("LocalDateTimeToRaw", $("f0")).cast(BINARY(13)),
                                serializeLocalDateTime(
                                        LocalDateTime.parse("2020-11-11T18:08:01.123")),
                                BINARY(13)));
    }
}
