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

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;

/** Template of a function intermediate result (i.e. state) or final result (i.e. output). */
@Internal
interface FunctionResultTemplate {

    static FunctionOutputTemplate ofOutput(DataType dataType) {
        return new FunctionOutputTemplate(dataType);
    }

    static FunctionStateTemplate ofState(LinkedHashMap<String, DataType> state) {
        return new FunctionStateTemplate(state);
    }

    @Internal
    class FunctionOutputTemplate implements FunctionResultTemplate {

        private final DataType dataType;

        private FunctionOutputTemplate(DataType dataType) {
            this.dataType = dataType;
        }

        TypeStrategy toTypeStrategy() {
            return TypeStrategies.explicit(dataType);
        }

        Class<?> toClass() {
            return dataType.getConversionClass();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final FunctionOutputTemplate template = (FunctionOutputTemplate) o;
            return Objects.equals(dataType, template.dataType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataType);
        }
    }

    @Internal
    class FunctionStateTemplate implements FunctionResultTemplate {

        private final LinkedHashMap<String, DataType> state;

        private FunctionStateTemplate(LinkedHashMap<String, DataType> state) {
            this.state = state;
        }

        List<Class<?>> toClassList() {
            return state.values().stream()
                    .map(DataType::getConversionClass)
                    .collect(Collectors.toList());
        }

        LinkedHashMap<String, StateTypeStrategy> toStateTypeStrategies() {
            return state.entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> createStateTypeStrategy(e.getValue()),
                                    (o, n) -> o,
                                    LinkedHashMap::new));
        }

        String toAccumulatorStateName() {
            checkSingleStateEntry();
            return state.keySet().iterator().next();
        }

        TypeStrategy toAccumulatorTypeStrategy() {
            checkSingleStateEntry();
            return createTypeStrategy(state.values().iterator().next());
        }

        private void checkSingleStateEntry() {
            if (state.size() != 1) {
                throw extractionError("Aggregating functions support only one state entry.");
            }
        }

        private static StateTypeStrategy createStateTypeStrategy(DataType dataType) {
            return StateTypeStrategy.of(TypeStrategies.explicit(dataType));
        }

        private static TypeStrategy createTypeStrategy(DataType dataType) {
            return TypeStrategies.explicit(dataType);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final FunctionStateTemplate that = (FunctionStateTemplate) o;
            return Objects.equals(state, that.state);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state);
        }
    }
}
