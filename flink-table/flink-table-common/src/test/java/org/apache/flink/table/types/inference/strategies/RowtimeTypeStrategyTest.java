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
import org.apache.flink.table.types.inference.TypeStrategiesTestBase;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.stream.Stream;

/** Tests for {@link RowtimeTypeStrategy}. */
class RowtimeTypeStrategyTest extends TypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy("TIMESTAMP(3) *ROWTIME*", SpecificTypeStrategies.ROWTIME)
                        .inputTypes(createRowtimeType(TimestampKind.ROWTIME, 3).notNull())
                        .expectDataType(createRowtimeType(TimestampKind.ROWTIME, 3).notNull()),
                TestSpec.forStrategy("TIMESTAMP_LTZ(3) *ROWTIME*", SpecificTypeStrategies.ROWTIME)
                        .inputTypes(createRowtimeLtzType(TimestampKind.ROWTIME, 3).notNull())
                        .expectDataType(createRowtimeLtzType(TimestampKind.ROWTIME, 3).notNull()),
                TestSpec.forStrategy("BIGINT", SpecificTypeStrategies.ROWTIME)
                        .inputTypes(DataTypes.BIGINT().notNull())
                        .expectDataType(DataTypes.TIMESTAMP(3).notNull()));
    }

    static DataType createRowtimeType(TimestampKind kind, int precision) {
        return TypeConversions.fromLogicalToDataType(new TimestampType(false, kind, precision));
    }

    static DataType createRowtimeLtzType(TimestampKind kind, int precision) {
        return TypeConversions.fromLogicalToDataType(
                new LocalZonedTimestampType(false, kind, precision));
    }
}
