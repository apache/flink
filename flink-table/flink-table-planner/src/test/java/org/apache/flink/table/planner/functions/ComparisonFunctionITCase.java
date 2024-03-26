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
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Tests for comparison functions such as {@link BuiltInFunctionDefinitions#EQUALS}, {@link
 * BuiltInFunctionDefinitions#GREATER_THAN} etc.
 */
public class ComparisonFunctionITCase extends BuiltInFunctionTestBase {

    @Override
    Configuration getConfiguration() {
        final Configuration config = new Configuration();
        // make the LTZ stable across all environments
        config.set(TableConfigOptions.LOCAL_TIME_ZONE, "GMT-08:00");
        return config;
    }

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        final Instant ltz3 = Instant.ofEpochMilli(1_123);
        final Instant ltz0 = Instant.ofEpochMilli(1_000);
        final LocalDateTime tmstmp3 = ltz3.atOffset(ZoneOffset.ofHours(-8)).toLocalDateTime();
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.EQUALS)
                        .onFieldsWithData(ltz3, ltz0, tmstmp3)
                        .andDataTypes(TIMESTAMP_LTZ(3), TIMESTAMP_LTZ(0), TIMESTAMP(3))
                        // compare same type, but different precision, should always adjust to the
                        // higher precision
                        .testResult($("f0").isEqual($("f1")), "f0 = f1", false, DataTypes.BOOLEAN())
                        .testResult($("f1").isEqual($("f0")), "f1 = f0", false, DataTypes.BOOLEAN())
                        // compare different types
                        .testResult($("f0").isEqual($("f2")), "f0 = f2", true, DataTypes.BOOLEAN())
                        .testResult($("f2").isEqual($("f0")), "f2 = f0", true, DataTypes.BOOLEAN())
                        // compare different type and different precision, should always adjust to
                        // the higher precision
                        .testResult($("f1").isEqual($("f2")), "f1 = f2", false, DataTypes.BOOLEAN())
                        .testResult(
                                $("f2").isEqual($("f1")), "f2 = f1", false, DataTypes.BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.GREATER_THAN)
                        .onFieldsWithData(ltz3, ltz0, tmstmp3.minusSeconds(1))
                        .andDataTypes(TIMESTAMP_LTZ(3), TIMESTAMP_LTZ(0), TIMESTAMP(3))
                        // compare same type, but different precision
                        .testResult(
                                $("f0").isGreater($("f1")), "f0 > f1", true, DataTypes.BOOLEAN())
                        .testResult(
                                $("f1").isGreater($("f0")), "f1 > f0", false, DataTypes.BOOLEAN())
                        // compare different types, same precision
                        .testResult(
                                $("f0").isGreater($("f2")), "f0 > f2", true, DataTypes.BOOLEAN())
                        .testResult(
                                $("f2").isGreater($("f0")), "f2 > f0", false, DataTypes.BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.LESS_THAN)
                        .onFieldsWithData(ltz3, ltz0, tmstmp3.minusSeconds(1))
                        .andDataTypes(TIMESTAMP_LTZ(3), TIMESTAMP_LTZ(0), TIMESTAMP(3))
                        // compare same type, but different precision
                        .testResult($("f0").isLess($("f1")), "f0 < f1", false, DataTypes.BOOLEAN())
                        .testResult($("f1").isLess($("f0")), "f1 < f0", true, DataTypes.BOOLEAN())
                        // compare different types, same precision
                        .testResult($("f0").isLess($("f2")), "f0 < f2", false, DataTypes.BOOLEAN())
                        .testResult($("f2").isLess($("f0")), "f2 < f0", true, DataTypes.BOOLEAN()));
    }
}
