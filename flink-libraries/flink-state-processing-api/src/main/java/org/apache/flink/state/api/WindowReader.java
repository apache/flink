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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.functions.WindowReaderFunction;
import org.apache.flink.state.api.input.KeyedStateInputFormat;
import org.apache.flink.state.api.input.operator.WindowReaderOperator;
import org.apache.flink.state.api.input.operator.window.PassThroughReader;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * This class provides entry points for reading keyed state written out using the {@code
 * WindowOperator}.
 *
 * @param <W> The type of {@code Window}.
 */
@PublicEvolving
public class WindowReader<W extends Window> {

    /** The batch execution environment. Used for creating inputs for reading state. */
    private final ExecutionEnvironment env;

    /**
     * The savepoint metadata, which maintains the current set of existing / newly added operator
     * states.
     */
    private final SavepointMetadata metadata;

    /**
     * The state backend that was previously used to write existing operator states in this
     * savepoint. This is also the state backend that will be used when writing again this existing
     * savepoint.
     */
    private final StateBackend stateBackend;

    /** The window serializer used to write the window operator. */
    private final TypeSerializer<W> windowSerializer;

    WindowReader(
            ExecutionEnvironment env,
            SavepointMetadata metadata,
            StateBackend stateBackend,
            TypeSerializer<W> windowSerializer) {
        Preconditions.checkNotNull(env, "The execution environment must not be null");
        Preconditions.checkNotNull(metadata, "The savepoint metadata must not be null");
        Preconditions.checkNotNull(stateBackend, "The state backend must not be null");
        Preconditions.checkNotNull(windowSerializer, "The window serializer must not be null");

        this.env = env;
        this.metadata = metadata;
        this.stateBackend = stateBackend;
        this.windowSerializer = windowSerializer;
    }

    /** Reads from a window that uses an evictor. */
    public EvictingWindowReader<W> evictor() {
        return new EvictingWindowReader<>(env, metadata, stateBackend, windowSerializer);
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
     * @return A {@code DataSet} of objects read from keyed state.
     * @throws IOException If savepoint does not contain the specified uid.
     */
    public <T, K> DataSet<T> reduce(
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
     * @return A {@code DataSet} of objects read from keyed state.
     * @throws IOException If savepoint does not contain the specified uid.
     */
    public <K, T, OUT> DataSet<OUT> reduce(
            String uid,
            ReduceFunction<T> function,
            WindowReaderFunction<T, OUT, K, W> readerFunction,
            TypeInformation<K> keyType,
            TypeInformation<T> reduceType,
            TypeInformation<OUT> outputType)
            throws IOException {

        WindowReaderOperator<?, K, T, W, OUT> operator =
                WindowReaderOperator.reduce(
                        function, readerFunction, keyType, windowSerializer, reduceType);

        return readWindowOperator(uid, outputType, operator);
    }

    /**
     * Reads window state generated using an {@link AggregateFunction}.
     *
     * @param uid The uid of the operator.
     * @param aggregateFunction The aggregate function used to create the window.
     * @param keyType The key type of the window.
     * @param accType The type information of the accumulator function.
     * @param outputType The output type of the reader function.
     * @param <K> The type of the key.
     * @param <T> The type of the values that are aggregated.
     * @param <ACC> The type of the accumulator (intermediate aggregate state).
     * @param <R> The type of the aggregated result.
     * @return A {@code DataSet} of objects read from keyed state.
     * @throws IOException If savepoint does not contain the specified uid.
     */
    public <K, T, ACC, R> DataSet<R> aggregate(
            String uid,
            AggregateFunction<T, ACC, R> aggregateFunction,
            TypeInformation<K> keyType,
            TypeInformation<ACC> accType,
            TypeInformation<R> outputType)
            throws IOException {

        return aggregate(
                uid, aggregateFunction, new PassThroughReader<>(), keyType, accType, outputType);
    }

    /**
     * Reads window state generated using an {@link AggregateFunction}.
     *
     * @param uid The uid of the operator.
     * @param aggregateFunction The aggregate function used to create the window.
     * @param readerFunction The window reader function.
     * @param keyType The key type of the window.
     * @param accType The type information of the accumulator function.
     * @param outputType The output type of the reader function.
     * @param <K> The type of the key.
     * @param <T> The type of the values that are aggregated.
     * @param <ACC> The type of the accumulator (intermediate aggregate state).
     * @param <R> The type of the aggregated result.
     * @param <OUT> The output type of the reader function.
     * @return A {@code DataSet} of objects read from keyed state.
     * @throws IOException If savepoint does not contain the specified uid.
     */
    public <K, T, ACC, R, OUT> DataSet<OUT> aggregate(
            String uid,
            AggregateFunction<T, ACC, R> aggregateFunction,
            WindowReaderFunction<R, OUT, K, W> readerFunction,
            TypeInformation<K> keyType,
            TypeInformation<ACC> accType,
            TypeInformation<OUT> outputType)
            throws IOException {

        WindowReaderOperator<?, K, R, W, OUT> operator =
                WindowReaderOperator.aggregate(
                        aggregateFunction, readerFunction, keyType, windowSerializer, accType);

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
     * @return A {@code DataSet} of objects read from keyed state.
     * @throws IOException If the savepoint does not contain the specified uid.
     */
    public <K, T, OUT> DataSet<OUT> process(
            String uid,
            WindowReaderFunction<T, OUT, K, W> readerFunction,
            TypeInformation<K> keyType,
            TypeInformation<T> stateType,
            TypeInformation<OUT> outputType)
            throws IOException {

        WindowReaderOperator<?, K, T, W, OUT> operator =
                WindowReaderOperator.process(readerFunction, keyType, windowSerializer, stateType);

        return readWindowOperator(uid, outputType, operator);
    }

    private <K, T, OUT> DataSet<OUT> readWindowOperator(
            String uid,
            TypeInformation<OUT> outputType,
            WindowReaderOperator<?, K, T, W, OUT> operator)
            throws IOException {
        KeyedStateInputFormat<K, W, OUT> format =
                new KeyedStateInputFormat<>(
                        metadata.getOperatorState(uid),
                        stateBackend,
                        env.getConfiguration(),
                        operator);

        return env.createInput(format, outputType);
    }
}
