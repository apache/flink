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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;

/**
 * Type strategy that maps an {@link InputTypeStrategy} to a {@link TypeStrategy} if the input
 * strategy infers compatible types.
 */
@Internal
public final class MappingTypeStrategy implements TypeStrategy {

    private final Map<InputTypeStrategy, TypeStrategy> mappings;

    public MappingTypeStrategy(Map<InputTypeStrategy, TypeStrategy> mappings) {
        this.mappings = mappings;
    }

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final List<LogicalType> actualTypes =
                callContext.getArgumentDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList());
        for (Map.Entry<InputTypeStrategy, TypeStrategy> mapping : mappings.entrySet()) {
            final InputTypeStrategy inputStrategy = mapping.getKey();
            final TypeStrategy strategy = mapping.getValue();
            if (!inputStrategy.getArgumentCount().isValidCount(actualTypes.size())) {
                continue;
            }
            final Optional<List<DataType>> inferredDataTypes =
                    inputStrategy.inferInputTypes(callContext, false);
            if (!inferredDataTypes.isPresent()) {
                continue;
            }
            final List<LogicalType> inferredTypes =
                    inferredDataTypes.get().stream()
                            .map(DataType::getLogicalType)
                            .collect(Collectors.toList());
            if (supportsAvoidingCast(actualTypes, inferredTypes)) {
                return strategy.inferType(callContext);
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MappingTypeStrategy that = (MappingTypeStrategy) o;
        return mappings.equals(that.mappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappings);
    }
}
