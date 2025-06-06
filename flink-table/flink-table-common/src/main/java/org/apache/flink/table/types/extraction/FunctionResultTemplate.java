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
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.FunctionResultTemplate.FunctionStateTemplate.StateInfoTemplate;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.util.TimeUtils;

import javax.annotation.Nullable;

import java.time.Duration;
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

    static FunctionStateTemplate ofState(LinkedHashMap<String, StateInfoTemplate> state) {
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

        private final LinkedHashMap<String, StateInfoTemplate> stateInfos;

        @Internal
        static class StateInfoTemplate {
            final DataType dataType;
            final @Nullable Duration ttl;

            private StateInfoTemplate(DataType dataType, @Nullable Duration ttl) {
                this.dataType = dataType;
                this.ttl = ttl;
            }

            static StateInfoTemplate of(DataType dataType, @Nullable StateHint stateHint) {
                // State hints have stricter requirements on the data type than accumulators
                if (stateHint != null) {
                    ExtractionUtils.checkStateDataType(dataType);
                }
                return new StateInfoTemplate(dataType, createStateTimeToLive(stateHint));
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                final StateInfoTemplate that = (StateInfoTemplate) o;
                return Objects.equals(dataType, that.dataType) && Objects.equals(ttl, that.ttl);
            }

            @Override
            public int hashCode() {
                return Objects.hash(dataType, ttl);
            }
        }

        private FunctionStateTemplate(LinkedHashMap<String, StateInfoTemplate> stateInfos) {
            this.stateInfos = stateInfos;
        }

        List<Class<?>> toClassList() {
            return stateInfos.values().stream()
                    .map(info -> info.dataType)
                    .map(DataType::getConversionClass)
                    .collect(Collectors.toList());
        }

        LinkedHashMap<String, StateTypeStrategy> toStateTypeStrategies() {
            return stateInfos.entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> createStateTypeStrategy(e.getValue()),
                                    (o, n) -> o,
                                    LinkedHashMap::new));
        }

        String toAccumulatorStateName() {
            checkSingleStateEntry();
            return stateInfos.keySet().iterator().next();
        }

        TypeStrategy toAccumulatorTypeStrategy() {
            checkSingleStateEntry();
            final StateInfoTemplate stateInfo = stateInfos.values().iterator().next();
            return createTypeStrategy(stateInfo.dataType);
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
            return Objects.equals(stateInfos, that.stateInfos);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stateInfos);
        }

        private void checkSingleStateEntry() {
            if (stateInfos.size() != 1) {
                throw extractionError("Aggregating functions support only one state entry.");
            }
        }

        private static StateTypeStrategy createStateTypeStrategy(StateInfoTemplate stateInfo) {
            return StateTypeStrategy.of(TypeStrategies.explicit(stateInfo.dataType), stateInfo.ttl);
        }

        private static TypeStrategy createTypeStrategy(DataType dataType) {
            return TypeStrategies.explicit(dataType);
        }

        private static @Nullable Duration createStateTimeToLive(@Nullable StateHint stateHint) {
            if (stateHint == null) {
                return null;
            }
            final String ttl = stateHint.ttl();
            try {
                if (ttl.isEmpty()) {
                    return null;
                }
                return TimeUtils.parseDuration(ttl);
            } catch (Exception e) {
                throw extractionError("Invalid TTL duration: %s", ttl);
            }
        }
    }
}
