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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.sortpartition.KeyedSortPartitionOperator;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link KeyedPartitionWindowedStream} represents a data stream that collects all records with the
 * same key separately into a full window.
 */
@Internal
public class KeyedPartitionWindowedStream<T, KEY> implements PartitionWindowedStream<T> {

    private final StreamExecutionEnvironment environment;

    private final KeyedStream<T, KEY> input;

    public KeyedPartitionWindowedStream(
            StreamExecutionEnvironment environment, KeyedStream<T, KEY> input) {
        this.environment = environment;
        this.input = input;
    }

    @Override
    public <R> SingleOutputStreamOperator<R> mapPartition(
            MapPartitionFunction<T, R> mapPartitionFunction) {
        checkNotNull(mapPartitionFunction, "The map partition function must not be null.");
        mapPartitionFunction = environment.clean(mapPartitionFunction);
        String opName = "MapPartition";
        TypeInformation<R> resultType =
                TypeExtractor.getMapPartitionReturnTypes(
                        mapPartitionFunction, input.getType(), opName, true);
        MapPartitionFunction<T, R> function = mapPartitionFunction;
        return input.window(GlobalWindows.createWithEndOfStreamTrigger())
                .apply(
                        new WindowFunction<T, R, KEY, GlobalWindow>() {
                            @Override
                            public void apply(
                                    KEY key,
                                    GlobalWindow window,
                                    Iterable<T> input,
                                    Collector<R> out)
                                    throws Exception {
                                function.mapPartition(input, out);
                            }
                        },
                        resultType);
    }

    @Override
    public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> reduceFunction) {
        checkNotNull(reduceFunction, "The reduce function must not be null.");
        reduceFunction = environment.clean(reduceFunction);
        return input.window(GlobalWindows.createWithEndOfStreamTrigger()).reduce(reduceFunction);
    }

    @Override
    public <ACC, R> SingleOutputStreamOperator<R> aggregate(
            AggregateFunction<T, ACC, R> aggregateFunction) {
        checkNotNull(aggregateFunction, "The aggregate function must not be null.");
        aggregateFunction = environment.clean(aggregateFunction);
        return input.window(GlobalWindows.createWithEndOfStreamTrigger())
                .aggregate(aggregateFunction);
    }

    @Override
    public SingleOutputStreamOperator<T> sortPartition(int field, Order order) {
        checkNotNull(order, "The order must not be null.");
        checkArgument(field > 0, "The field mustn't be less than zero.");
        TypeInformation<T> inputType = input.getType();
        KeyedSortPartitionOperator<T, KEY> operator =
                new KeyedSortPartitionOperator<>(inputType, field, order);
        final String opName = "KeyedSortPartition";
        SingleOutputStreamOperator<T> result =
                this.input
                        .transform(opName, inputType, operator)
                        .setParallelism(input.getParallelism());
        int managedMemoryWeight =
                Math.max(
                        1,
                        environment
                                .getConfiguration()
                                .get(ExecutionOptions.SORT_KEYED_PARTITION_MEMORY)
                                .getMebiBytes());
        result.getTransformation()
                .declareManagedMemoryUseCaseAtOperatorScope(
                        ManagedMemoryUseCase.OPERATOR, managedMemoryWeight);
        return result;
    }

    @Override
    public SingleOutputStreamOperator<T> sortPartition(String field, Order order) {
        checkNotNull(field, "The field must not be null.");
        checkNotNull(order, "The order must not be null.");
        TypeInformation<T> inputType = input.getType();
        KeyedSortPartitionOperator<T, KEY> operator =
                new KeyedSortPartitionOperator<>(inputType, field, order);
        final String opName = "KeyedSortPartition";
        SingleOutputStreamOperator<T> result =
                this.input
                        .transform(opName, inputType, operator)
                        .setParallelism(input.getParallelism());
        int managedMemoryWeight =
                Math.max(
                        1,
                        environment
                                .getConfiguration()
                                .get(ExecutionOptions.SORT_KEYED_PARTITION_MEMORY)
                                .getMebiBytes());
        result.getTransformation()
                .declareManagedMemoryUseCaseAtOperatorScope(
                        ManagedMemoryUseCase.OPERATOR, managedMemoryWeight);
        return result;
    }

    @Override
    public <K> SingleOutputStreamOperator<T> sortPartition(
            KeySelector<T, K> keySelector, Order order) {
        checkNotNull(keySelector, "The field must not be null.");
        checkNotNull(order, "The order must not be null.");
        TypeInformation<T> inputType = input.getType();
        KeyedSortPartitionOperator<T, KEY> operator =
                new KeyedSortPartitionOperator<>(inputType, environment.clean(keySelector), order);
        final String opName = "KeyedSortPartition";
        SingleOutputStreamOperator<T> result =
                this.input
                        .transform(opName, inputType, operator)
                        .setParallelism(input.getParallelism());
        int managedMemoryWeight =
                Math.max(
                        1,
                        environment
                                .getConfiguration()
                                .get(ExecutionOptions.SORT_KEYED_PARTITION_MEMORY)
                                .getMebiBytes());
        result.getTransformation()
                .declareManagedMemoryUseCaseAtOperatorScope(
                        ManagedMemoryUseCase.OPERATOR, managedMemoryWeight);
        return result;
    }
}
