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
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Type strategy that returns the first type that could be inferred. */
@Internal
public final class FirstTypeStrategy implements TypeStrategy {

    private final List<? extends TypeStrategy> typeStrategies;

    public FirstTypeStrategy(List<? extends TypeStrategy> typeStrategies) {
        Preconditions.checkArgument(typeStrategies.size() > 0);
        this.typeStrategies = typeStrategies;
    }

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        for (TypeStrategy strategy : typeStrategies) {
            final Optional<DataType> inferredDataType = strategy.inferType(callContext);
            if (inferredDataType.isPresent()) {
                return inferredDataType;
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
        FirstTypeStrategy that = (FirstTypeStrategy) o;
        return typeStrategies.equals(that.typeStrategies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeStrategies);
    }
}
