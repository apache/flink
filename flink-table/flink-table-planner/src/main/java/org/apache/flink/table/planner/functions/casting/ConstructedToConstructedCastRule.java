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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.List;
import java.util.stream.IntStream;

/**
 * This interface provides a default implementation for {@link #canFail(LogicalType, LogicalType)}
 * for constructed type casts, e.g. ARRAY to ARRAY (but not ARRAY to STRING).
 */
interface ConstructedToConstructedCastRule<IN, OUT> extends CastRule<IN, OUT> {

    @Override
    default boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        final List<LogicalType> inputFields = LogicalTypeChecks.getFieldTypes(inputLogicalType);
        final List<LogicalType> targetFields = LogicalTypeChecks.getFieldTypes(targetLogicalType);

        // This should have been already checked when the rule is matched
        assert inputFields.size() == targetFields.size();

        return IntStream.range(0, inputFields.size())
                .anyMatch(i -> CastRuleProvider.canFail(inputFields.get(i), targetFields.get(i)));
    }
}
