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

package org.apache.flink.state.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.functions.WindowReaderFunction;
import org.apache.flink.state.api.input.KeyedStateInputFormat;
import org.apache.flink.state.api.input.SourceBuilder;
import org.apache.flink.state.api.input.operator.WindowReaderOperator;
import org.apache.flink.state.api.input.operator.window.AggregateEvictingWindowReaderFunction;
import org.apache.flink.state.api.input.operator.window.PassThroughReader;
import org.apache.flink.state.api.input.operator.window.ProcessEvictingWindowReader;
import org.apache.flink.state.api.input.operator.window.ReduceEvictingWindowReaderFunction;
import org.apache.flink.state.api.runtime.MutableConfig;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadataV2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * This class provides entry points for reading keyed state written out using the {@code
 * WindowOperator}.
 *
 * @param <W> The type of {@code Window}.
 */
@PublicEvolving
public class EvictingWindowSavepointReader<W extends Window> {

    /** The execution environment. Used for creating inputs for reading state. */
    private final StreamExecutionEnvironment env;

    /**
     * The savepoint metadata, which maintains the current set of existing / newly added operator
     * states.
     */
    private final SavepointMetadataV2 metadata;

    /**
     * The state backend that was previously used to write existing operator states in this
     * savepoint. If null, the reader will use the state backend defined via the cluster
     * configuration.
     */
    @Nullable private final StateBackend stateBackend;

    /** The window serializer used to write the window operator. */
    private final TypeSerializer<W> windowSerializer;

    EvictingWindowSavepointReader(
            StreamExecutionEnvironment env,
            SavepointMetadataV2 metadata,
            @Nullable StateBackend stateBackend,
            TypeSerializer<W> windowSerializer) {
        Preconditions.checkNotNull(env, "The execution environment must not be null");
        Preconditions.checkNotNull(metadata, "The savepoint metadata must not be null");
        Preconditions.checkNotNull(windowSerializer, "The window serializer must not be null");

        this.env = env;
        this.metadata = metadata;
        this.stateBackend = stateBackend;
        this.windowSerializer = windowSerializer;
    }

    /**
     * Reads window state generated using a {@link ReduceFunction}.
     *
     * @param uid The uid of the operator.
     * @param function The reduce function used to create the window.
     * @param keyType The key type of the window.
     * @param reduceType The type information of the reduce function.
     * @param <T> The type of the reduce function.
     * @param <K> The key type of the operator.
     * @return A {@code DataStream} of objects read from keyed state.
     * @throws IOException If savepoint does not contain the specified uid.
     */
    public <T, K> DataStream<T> reduce(
            String uid,
            ReduceFunction<T> function,
            TypeInformation<K> keyType,
            TypeInformation<T> reduceType)
            throws IOException {

        return reduce(uid, function, new PassThroughReader<>(), keyType, reduceType, reduceType);
    }

    /**
     * Reads window state generated using a {@link ReduceFunction}.
     *
     * @param uid The uid of the operator.
     * @param function The reduce function used to create the window.
     * @param readerFunction The window reader function.
     * @param keyType The key type of the window.
     * @param reduceType The type information of the reduce function.
     * @param outputType The output type of the reader function.
     * @param <K> The type of the key.
     * @param <T> The type of the reduce function.
     * @param <OUT> The output type of the reduce function.
     * @return A {@code DataStream} of objects read from keyed state.
     * @throws IOException If savepoint does not contain the specified uid.
     */
    public <K, T, OUT> DataStream<OUT> reduce(
            String uid,
            ReduceFunction<T> function,
            WindowReaderFunction<T, OUT, K, W> readerFunction,
            TypeInformation<K> keyType,
            TypeInformation<T> reduceType,
            TypeInformation<OUT> outputType)
            throws IOException {

        WindowReaderOperator<?, K, StreamRecord<T>, W, OUT> operator =
                WindowReaderOperator.evictingWindow(
                        new ReduceEvictingWindowReaderFunction<>(readerFunction, function),
                        keyType,
                        windowSerializer,
                        reduceType,
                        env.getConfig());

        return readWindowOperator(uid, outputType, operator);
    }

    /**
     * Reads window state generated using an {@link AggregateFunction}.
     *
     * @param uid The uid of the operator.
     * @param aggregateFunction The aggregate function used to create the window.
     * @param keyType The key type of the window.
     * @param inputType The type information of the accumulator function.
     * @param outputType The output type of the reader function.
     * @param <K> The type of the key.
     * @param <T> The type of the values that are aggregated.
     * @param <ACC> The type of the accumulator (intermediate aggregate state).
     * @param <R> The type of the aggregated result.
     * @return A {@code DataStream} of objects read from keyed state.
     * @throws IOException If savepoint does not contain the specified uid.
     */
    public <K, T, ACC, R> DataStream<R> aggregate(
            String uid,
            AggregateFunction<T, ACC, R> aggregateFunction,
            TypeInformation<K> keyType,
            TypeInformation<T> inputType,
            TypeInformation<R> outputType)
            throws IOException {

        return aggregate(
                uid, aggregateFunction, new PassThroughReader<>(), keyType, inputType, outputType);
    }

    /**
     * Reads window state generated using an {@link AggregateFunction}.
     *
     * @param uid The uid of the operator.
     * @param aggregateFunction The aggregate function used to create the window.
     * @param readerFunction The window reader function.
     * @param keyType The key type of the window.
     * @param inputType The type information of the accumulator function.
     * @param outputType The output type of the reader function.
     * @param <K> The type of the key.
     * @param <T> The type of the values that are aggregated.
     * @param <ACC> The type of the accumulator (intermediate aggregate state).
     * @param <R> The type of the aggregated result.
     * @param <OUT> The output type of the reader function.
     * @return A {@code DataStream} of objects read from keyed state.
     * @throws IOException If savepoint does not contain the specified uid.
     */
    public <K, T, ACC, R, OUT> DataStream<OUT> aggregate(
            String uid,
            AggregateFunction<T, ACC, R> aggregateFunction,
            WindowReaderFunction<R, OUT, K, W> readerFunction,
            TypeInformation<K> keyType,
            TypeInformation<T> inputType,
            TypeInformation<OUT> outputType)
            throws IOException {

        WindowReaderOperator<?, K, StreamRecord<T>, W, OUT> operator =
                WindowReaderOperator.evictingWindow(
                        new AggregateEvictingWindowReaderFunction<>(
                                readerFunction, aggregateFunction),
                        keyType,
                        windowSerializer,
                        inputType,
                        env.getConfig());

        return readWindowOperator(uid, outputType, operator);
    }

    /**
     * Reads window state generated without any preaggregation such as {@code WindowedStream#apply}
     * and {@code WindowedStream#process}.
     *
     * @param uid The uid of the operator.
     * @param readerFunction The window reader function.
     * @param keyType The key type of the window.
     * @param stateType The type of records stored in state.
     * @param outputType The output type of the reader function.
     * @param <K> The type of the key.
     * @param <T> The type of the records stored in state.
     * @param <OUT> The output type of the reader function.
     * @return A {@code DataStream} of objects read from keyed state.
     * @throws IOException If the savepoint does not contain the specified uid.
     */
    public <K, T, OUT> DataStream<OUT> process(
            String uid,
            WindowReaderFunction<T, OUT, K, W> readerFunction,
            TypeInformation<K> keyType,
            TypeInformation<T> stateType,
            TypeInformation<OUT> outputType)
            throws IOException {

        WindowReaderOperator<?, K, StreamRecord<T>, W, OUT> operator =
                WindowReaderOperator.evictingWindow(
                        new ProcessEvictingWindowReader<>(readerFunction),
                        keyType,
                        windowSerializer,
                        stateType,
                        env.getConfig());

        return readWindowOperator(uid, outputType, operator);
    }

    private <K, T, OUT> DataStream<OUT> readWindowOperator(
            String uid,
            TypeInformation<OUT> outputType,
            WindowReaderOperator<?, K, T, W, OUT> operator)
            throws IOException {
        KeyedStateInputFormat<K, W, OUT> format =
                new KeyedStateInputFormat<>(
                        metadata.getOperatorState(OperatorIdentifier.forUid(uid)),
                        stateBackend,
                        MutableConfig.of(env.getConfiguration()),
                        operator);

        return SourceBuilder.fromFormat(env, format, outputType);
    }
}
