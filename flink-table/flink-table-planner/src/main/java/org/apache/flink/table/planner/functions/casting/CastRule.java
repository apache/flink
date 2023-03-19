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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.types.logical.LogicalType;

import java.time.ZoneId;

/**
 * A {@link CastRule} provides the logic to create a {@link CastExecutor} starting from the input
 * and the target types. A rule is matched using {@link CastRulePredicate}.
 *
 * @param <IN> Input internal type
 * @param <OUT> Output internal type
 */
@Internal
public interface CastRule<IN, OUT> {

    /** @see CastRulePredicate for more details about a cast rule predicate definition. */
    CastRulePredicate getPredicateDefinition();

    /**
     * Create a {@link CastExecutor} starting from the provided input type. The returned {@link
     * CastExecutor} assumes the input value is using the internal data type, and it's a valid value
     * for the provided {@code targetLogicalType}.
     */
    CastExecutor<IN, OUT> create(
            Context context, LogicalType inputLogicalType, LogicalType targetLogicalType);

    /** Returns true if the {@link CastExecutor} can fail at runtime. */
    default boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return false;
    }

    /** Casting context. */
    interface Context {

        boolean isPrinting();

        @Deprecated
        boolean legacyBehaviour();

        ZoneId getSessionZoneId();

        ClassLoader getClassLoader();

        /** Create a casting context. */
        static Context create(
                boolean isPrinting,
                boolean legacyBehaviour,
                ZoneId zoneId,
                ClassLoader classLoader) {
            return new Context() {
                @Override
                public boolean isPrinting() {
                    return isPrinting;
                }

                @Override
                public boolean legacyBehaviour() {
                    return legacyBehaviour;
                }

                @Override
                public ZoneId getSessionZoneId() {
                    return zoneId;
                }

                @Override
                public ClassLoader getClassLoader() {
                    return classLoader;
                }
            };
        }
    }
}
