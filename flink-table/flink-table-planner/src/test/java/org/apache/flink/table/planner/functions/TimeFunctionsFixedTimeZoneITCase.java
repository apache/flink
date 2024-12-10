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

import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.Expressions.$;

/** Test time-related built-in functions with fixed time zone. */
class TimeFunctionsFixedTimeZoneITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return Stream.of(ceilTestCases(), floorTestCases()).flatMap(s -> s);
    }

    private Stream<TestSetSpec> ceilTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.FLOOR)
                        .onFieldsWithData(
                                // https://issues.apache.org/jira/browse/FLINK-17224
                                // Fractional seconds are lost
                                LocalDateTime.of(2020, 2, 29, 1, 56, 57, 987654321))
                        .andDataTypes(TIMESTAMP())
                        .testResult(
                                $("f0").cast(TIMESTAMP_LTZ(3)).ceil(TimeIntervalUnit.MINUTE),
                                "CEIL(CAST(f0 AS TIMESTAMP_LTZ(3)) TO MINUTE)",
                                ZonedDateTime.of(
                                                LocalDateTime.of(2020, 2, 29, 1, 57, 0),
                                                ZoneId.of("UTC"))
                                        .toInstant(),
                                TIMESTAMP_LTZ(3))
                        .testResult(
                                $("f0").cast(TIMESTAMP_LTZ(3)).ceil(TimeIntervalUnit.SECOND),
                                "CEIL(CAST(f0 AS TIMESTAMP_LTZ(3)) TO SECOND)",
                                ZonedDateTime.of(
                                                LocalDateTime.of(2020, 2, 29, 1, 56, 58),
                                                ZoneId.of("UTC"))
                                        .toInstant(),
                                TIMESTAMP_LTZ(3)));
    }

    private Stream<TestSetSpec> floorTestCases() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.FLOOR)
                        .onFieldsWithData(
                                // https://issues.apache.org/jira/browse/FLINK-17224
                                // Fractional seconds are lost
                                LocalDateTime.of(2020, 2, 29, 1, 56, 59, 987654321))
                        .andDataTypes(TIMESTAMP())
                        .testResult(
                                $("f0").cast(TIMESTAMP_LTZ(3)).floor(TimeIntervalUnit.SECOND),
                                "FLOOR(CAST(f0 AS TIMESTAMP_LTZ(3)) TO SECOND)",
                                ZonedDateTime.of(
                                                LocalDateTime.of(2020, 2, 29, 1, 56, 59),
                                                ZoneId.of("UTC"))
                                        .toInstant(),
                                TIMESTAMP_LTZ(3))
                        .testResult(
                                $("f0").cast(TIMESTAMP_LTZ(3)).floor(TimeIntervalUnit.MINUTE),
                                "FLOOR(CAST(f0 AS TIMESTAMP_LTZ(3)) TO MINUTE)",
                                ZonedDateTime.of(
                                                LocalDateTime.of(2020, 2, 29, 1, 56, 0),
                                                ZoneId.of("UTC"))
                                        .toInstant(),
                                TIMESTAMP_LTZ(3)));
    }
}
