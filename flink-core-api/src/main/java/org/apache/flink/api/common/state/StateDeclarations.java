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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeDescriptor;

import java.io.Serializable;

/** This is a helper class for declaring various states. */
@Experimental
public class StateDeclarations {

    /** Get the builder of {@link AggregatingStateDeclaration}. */
    public static <IN, OUT, ACC>
            AggregatingStateDeclarationBuilder<IN, OUT, ACC> aggregatingStateBuilder(
                    String name,
                    TypeDescriptor<ACC> aggTypeDescriptor,
                    AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        return new AggregatingStateDeclarationBuilder<>(name, aggTypeDescriptor, aggregateFunction);
    }

    /** Get the builder of {@link ReducingStateDeclaration}. */
    public static <T> ReducingStateDeclarationBuilder<T> reducingStateBuilder(
            String name, TypeDescriptor<T> typeInformation, ReduceFunction<T> reduceFunction) {
        return new ReducingStateDeclarationBuilder<>(name, typeInformation, reduceFunction);
    }

    /** Get the builder of {@link MapStateDeclaration}. */
    public static <K, V> MapStateDeclarationBuilder<K, V> mapStateBuilder(
            String name,
            TypeDescriptor<K> keyTypeInformation,
            TypeDescriptor<V> valueTypeInformation) {
        return new MapStateDeclarationBuilder<>(name, keyTypeInformation, valueTypeInformation);
    }

    /** Get the builder of {@link ListStateDeclaration}. */
    public static <T> ListStateDeclarationBuilder<T> listStateBuilder(
            String name, TypeDescriptor<T> elementTypeInformation) {
        return new ListStateDeclarationBuilder<>(name, elementTypeInformation);
    }

    /** Get the builder of {@link ValueStateDeclaration}. */
    public <T> ValueStateDeclarationBuilder<T> valueStateBuilder(
            String name, TypeDescriptor<T> valueType) {
        return new ValueStateDeclarationBuilder<>(name, valueType);
    }

    /**
     * Get the {@link AggregatingStateDeclaration} of aggregating state. If you want to configure it
     * more elaborately, use {@link #aggregatingStateBuilder(String, TypeDescriptor,
     * AggregateFunction)}.
     */
    public static <IN, ACC, OUT> AggregatingStateDeclaration<IN, ACC, OUT> aggregatingState(
            String name,
            TypeDescriptor<ACC> aggTypeDescriptor,
            AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        return new AggregatingStateDeclarationBuilder<>(name, aggTypeDescriptor, aggregateFunction)
                .build();
    }

    /**
     * Get the {@link ReducingStateDeclaration} of list state. If you want to configure it more
     * elaborately, use {@link StateDeclarations#reducingStateBuilder(String, TypeDescriptor,
     * ReduceFunction)}.
     */
    public static <T> ReducingStateDeclaration<T> reducingState(
            String name, TypeDescriptor<T> typeInformation, ReduceFunction<T> reduceFunction) {
        return new ReducingStateDeclarationBuilder<>(name, typeInformation, reduceFunction).build();
    }

    /**
     * Get the {@link MapStateDeclaration} of map state with {@link
     * StateDeclaration.RedistributionMode#NONE}. If you want to configure it more elaborately, use
     * {@link StateDeclarations#mapStateBuilder(String, TypeDescriptor, TypeDescriptor)}.
     */
    public static <K, V> MapStateDeclaration<K, V> mapState(
            String name,
            TypeDescriptor<K> keyTypeInformation,
            TypeDescriptor<V> valueTypeInformation) {
        return new MapStateDeclarationBuilder<>(name, keyTypeInformation, valueTypeInformation)
                .build();
    }

    /**
     * Get the {@link ListStateDeclaration} of list state with {@link
     * StateDeclaration.RedistributionMode#NONE}. If you want to configure it more elaborately, use
     * {@link StateDeclarations#listStateBuilder(String, TypeDescriptor)}.
     */
    public static <T> ListStateDeclaration<T> listState(
            String name, TypeDescriptor<T> elementTypeInformation) {
        return new ListStateDeclarationBuilder<>(name, elementTypeInformation).build();
    }

    /**
     * Get the {@link ValueStateDeclaration} of value state. If you want to configure it more
     * elaborately, use {@link StateDeclarations#valueStateBuilder(String, TypeDescriptor)}.
     */
    public static <T> ValueStateDeclaration<T> valueState(
            String name, TypeDescriptor<T> valueType) {
        return new ValueStateDeclarationBuilder<>(name, valueType).build();
    }

    /** Builder for {@link ReducingStateDeclaration}. */
    @Experimental
    public static class ReducingStateDeclarationBuilder<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String name;
        private final TypeDescriptor<T> typeInformation;

        private final ReduceFunction<T> reduceFunction;

        public ReducingStateDeclarationBuilder(
                String name, TypeDescriptor<T> typeInformation, ReduceFunction<T> reduceFunction) {
            this.name = name;
            this.typeInformation = typeInformation;
            this.reduceFunction = reduceFunction;
        }

        ReducingStateDeclaration<T> build() {
            return new ReducingStateDeclaration<T>() {
                @Override
                public TypeDescriptor<T> getTypeDescriptor() {
                    return typeInformation;
                }

                @Override
                public String getName() {
                    return name;
                }

                @Override
                public ReduceFunction<T> getReduceFunction() {
                    return reduceFunction;
                }

                @Override
                public RedistributionMode getRedistributionMode() {
                    return RedistributionMode.NONE;
                }
            };
        }
    }

    /** Builder for {@link AggregatingStateDeclaration}. */
    @Experimental
    public static class AggregatingStateDeclarationBuilder<IN, OUT, ACC> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String name;

        private final TypeDescriptor<ACC> stateTypeDescriptor;
        private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

        public AggregatingStateDeclarationBuilder(
                String name,
                TypeDescriptor<ACC> stateTypeDescriptor,
                AggregateFunction<IN, ACC, OUT> aggregateFunction) {
            this.name = name;
            this.stateTypeDescriptor = stateTypeDescriptor;
            this.aggregateFunction = aggregateFunction;
        }

        AggregatingStateDeclaration<IN, ACC, OUT> build() {
            return new AggregatingStateDeclaration<IN, ACC, OUT>() {
                @Override
                public TypeDescriptor<ACC> getTypeDescriptor() {
                    return stateTypeDescriptor;
                }

                @Override
                public AggregateFunction<IN, ACC, OUT> getAggregateFunction() {
                    return aggregateFunction;
                }

                @Override
                public String getName() {
                    return name;
                }

                @Override
                public RedistributionMode getRedistributionMode() {
                    return RedistributionMode.NONE;
                }
            };
        }
    }

    /** Builder for {@link MapStateDeclaration}. */
    @Experimental
    public static class MapStateDeclarationBuilder<K, V> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String name;
        private final TypeDescriptor<K> keyTypeInformation;
        private final TypeDescriptor<V> valueTypeInformation;

        private final StateDeclaration.RedistributionMode redistributionMode;

        public MapStateDeclarationBuilder(
                String name,
                TypeDescriptor<K> keyTypeInformation,
                TypeDescriptor<V> valueTypeInformation) {
            this(
                    name,
                    keyTypeInformation,
                    valueTypeInformation,
                    StateDeclaration.RedistributionMode.NONE);
        }

        public MapStateDeclarationBuilder(
                String name,
                TypeDescriptor<K> keyTypeInformation,
                TypeDescriptor<V> valueTypeInformation,
                StateDeclaration.RedistributionMode redistributionMode) {
            this.name = name;
            this.keyTypeInformation = keyTypeInformation;
            this.valueTypeInformation = valueTypeInformation;
            this.redistributionMode = redistributionMode;
        }

        public BroadcastStateDeclaration<K, V> buildBroadcast() {

            return new BroadcastStateDeclaration<K, V>() {
                @Override
                public TypeDescriptor<K> getKeyTypeDescriptor() {
                    return keyTypeInformation;
                }

                @Override
                public TypeDescriptor<V> getValueTypeDescriptor() {
                    return valueTypeInformation;
                }

                @Override
                public String getName() {
                    return name;
                }

                @Override
                public RedistributionMode getRedistributionMode() {
                    return StateDeclaration.RedistributionMode.IDENTICAL;
                }
            };
        }

        MapStateDeclaration<K, V> build() {
            return new MapStateDeclaration<K, V>() {
                @Override
                public TypeDescriptor<K> getKeyTypeDescriptor() {
                    return keyTypeInformation;
                }

                @Override
                public TypeDescriptor<V> getValueTypeDescriptor() {
                    return valueTypeInformation;
                }

                @Override
                public String getName() {
                    return name;
                }

                @Override
                public RedistributionMode getRedistributionMode() {
                    return redistributionMode;
                }
            };
        }
    }

    /** Builder for {@link ListStateDeclaration}. */
    @Experimental
    public static class ListStateDeclarationBuilder<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String name;
        private final TypeDescriptor<T> elementTypeInformation;
        private ListStateDeclaration.RedistributionStrategy redistributionStrategy =
                ListStateDeclaration.RedistributionStrategy.SPLIT;
        private StateDeclaration.RedistributionMode redistributionMode =
                StateDeclaration.RedistributionMode.NONE;

        public ListStateDeclarationBuilder(String name, TypeDescriptor<T> elementTypeInformation) {
            this.name = name;
            this.elementTypeInformation = elementTypeInformation;
        }

        public ListStateDeclarationBuilder<T> redistributeBy(
                ListStateDeclaration.RedistributionStrategy strategy) {
            this.redistributionStrategy = strategy;
            this.redistributionMode = StateDeclaration.RedistributionMode.REDISTRIBUTABLE;
            return this;
        }

        public ListStateDeclarationBuilder<T> redistributeWithMode(
                StateDeclaration.RedistributionMode mode) {
            this.redistributionMode = mode;
            return this;
        }

        public ListStateDeclaration<T> build() {

            return new ListStateDeclaration<T>() {
                @Override
                public RedistributionStrategy getRedistributionStrategy() {
                    return redistributionStrategy;
                }

                @Override
                public TypeDescriptor<T> getTypeDescriptor() {
                    return elementTypeInformation;
                }

                @Override
                public String getName() {
                    return name;
                }

                @Override
                public RedistributionMode getRedistributionMode() {
                    return redistributionMode;
                }
            };
        }
    }

    /** Builder for {@link ValueStateDeclaration}. */
    @Experimental
    public static class ValueStateDeclarationBuilder<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String name;
        private final TypeDescriptor<T> valueType;

        public ValueStateDeclarationBuilder(String name, TypeDescriptor<T> valueType) {
            this.name = name;
            this.valueType = valueType;
        }

        ValueStateDeclaration<T> build() {
            return new ValueStateDeclaration<T>() {
                @Override
                public TypeDescriptor<T> getTypeDescriptor() {
                    return valueType;
                }

                @Override
                public String getName() {
                    return name;
                }

                @Override
                public RedistributionMode getRedistributionMode() {
                    return RedistributionMode.NONE;
                }
            };
        }
    }
}
