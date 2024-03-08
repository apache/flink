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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.MapPartitionOperator;
import org.apache.flink.streaming.api.operators.PartitionAggregateOperator;
import org.apache.flink.streaming.api.operators.PartitionReduceOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link NonKeyedPartitionWindowedStream} represents a data stream that collects all records of
 * each subtask separately into a full window.
 */
@Internal
public class NonKeyedPartitionWindowedStream<T> implements PartitionWindowedStream<T> {

    private final StreamExecutionEnvironment environment;

    private final DataStream<T> input;

    public NonKeyedPartitionWindowedStream(
            StreamExecutionEnvironment environment, DataStream<T> input) {
        this.environment = environment;
        this.input = input;
    }

    @Override
    public <R> SingleOutputStreamOperator<R> mapPartition(
            MapPartitionFunction<T, R> mapPartitionFunction) {
        if (mapPartitionFunction == null) {
            throw new NullPointerException("The map partition function must not be null.");
        }
        mapPartitionFunction = environment.clean(mapPartitionFunction);
        String opName = "MapPartition";
        TypeInformation<R> resultType =
                TypeExtractor.getMapPartitionReturnTypes(
                        mapPartitionFunction, input.getType(), opName, true);
        return input.setConnectionType(new ForwardPartitioner<>())
                .transform(opName, resultType, new MapPartitionOperator<>(mapPartitionFunction))
                .setParallelism(input.getParallelism());
    }

    @Override
    public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> reduceFunction) {
        if (reduceFunction == null) {
            throw new IllegalArgumentException("The reduce function must not be null.");
        }
        reduceFunction = environment.clean(reduceFunction);
        String opName = "PartitionReduce";
        return input.setConnectionType(new ForwardPartitioner<>())
                .transform(
                        opName,
                        input.getTransformation().getOutputType(),
                        new PartitionReduceOperator<>(reduceFunction))
                .setParallelism(input.getParallelism());
    }

    @Override
    public <ACC, R> SingleOutputStreamOperator<R> aggregate(
            AggregateFunction<T, ACC, R> aggregateFunction) {
        checkNotNull(aggregateFunction, "The aggregate function must not be null.");
        aggregateFunction = environment.clean(aggregateFunction);
        String opName = "PartitionAggregate";
        TypeInformation<R> resultType =
                TypeExtractor.getAggregateFunctionReturnType(
                        aggregateFunction, input.getType(), opName, true);
        return input.setConnectionType(new ForwardPartitioner<>())
                .transform(opName, resultType, new PartitionAggregateOperator<>(aggregateFunction))
                .setParallelism(input.getParallelism());
    }
}
