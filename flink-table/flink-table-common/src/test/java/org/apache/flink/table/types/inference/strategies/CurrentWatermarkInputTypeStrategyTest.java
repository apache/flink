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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.runners.Parameterized;

import java.util.List;

import static java.util.Arrays.asList;

/** Tests for {@link CurrentWatermarkInputTypeStrategy}. */
public class CurrentWatermarkInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return asList(
                TestSpec.forStrategy(
                                "TIMESTAMP(3) *ROWTIME* works",
                                SpecificInputTypeStrategies.CURRENT_WATERMARK)
                        .calledWithArgumentTypes(createRowtimeType(TimestampKind.ROWTIME, 3))
                        .expectArgumentTypes(createRowtimeType(TimestampKind.ROWTIME, 3)),
                TestSpec.forStrategy(
                                "TIMESTAMP_LTZ(3) *ROWTIME* works",
                                SpecificInputTypeStrategies.CURRENT_WATERMARK)
                        .calledWithArgumentTypes(createRowtimeLtzType(TimestampKind.ROWTIME, 3))
                        .expectArgumentTypes(createRowtimeLtzType(TimestampKind.ROWTIME, 3)),
                TestSpec.forStrategy(
                                "TIMESTAMP(3) doesn't work",
                                SpecificInputTypeStrategies.CURRENT_WATERMARK)
                        .calledWithArgumentTypes(createRowtimeType(TimestampKind.REGULAR, 3))
                        .expectErrorMessage(
                                "The argument of CURRENT_WATERMARK() must be a rowtime attribute, but was 'TIMESTAMP(3) NOT NULL'."),
                TestSpec.forStrategy(
                                "TIMESTAMP_LTZ(3) doesn't work",
                                SpecificInputTypeStrategies.CURRENT_WATERMARK)
                        .calledWithArgumentTypes(createRowtimeLtzType(TimestampKind.REGULAR, 3))
                        .expectErrorMessage(
                                "The argument of CURRENT_WATERMARK() must be a rowtime attribute, but was 'TIMESTAMP_LTZ(3) NOT NULL'."),
                TestSpec.forStrategy(
                                "BIGINT doesn't work",
                                SpecificInputTypeStrategies.CURRENT_WATERMARK)
                        .calledWithArgumentTypes(DataTypes.BIGINT())
                        .expectErrorMessage(
                                "CURRENT_WATERMARK() must be called with a single rowtime attribute argument, but 'BIGINT' cannot be a time attribute."));
    }

    private static DataType createRowtimeType(TimestampKind kind, int precision) {
        return TypeConversions.fromLogicalToDataType(new TimestampType(false, kind, precision));
    }

    private static DataType createRowtimeLtzType(TimestampKind kind, int precision) {
        return TypeConversions.fromLogicalToDataType(
                new LocalZonedTimestampType(false, kind, precision));
    }
}
