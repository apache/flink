package org.apache.flink.table.types.inference.strategies;

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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.Optional;

/**
 * Placeholder for an unused type strategy.
 *
 * <p>For some internal helper functions the planner does not use a type inference. For example,
 * {@link BuiltInFunctionDefinitions#INTERNAL_UNNEST_ROWS}). Those functions differ from {@link
 * TypeStrategies#MISSING} in the sense that there is neither a Flink stack type inference nor a
 * Calcite stack type inference being used. Instead, the types are created by a planner rule.
 */
@Internal
final class UnusedTypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof UnusedTypeStrategy;
    }

    @Override
    public int hashCode() {
        return UnusedTypeStrategy.class.hashCode();
    }
}
