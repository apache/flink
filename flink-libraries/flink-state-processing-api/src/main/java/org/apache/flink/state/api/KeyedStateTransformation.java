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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.output.operators.KeyedStateBootstrapOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.OptionalInt;

/**
 * A {@link KeyedStateTransformation} represents a {@link OneInputStateTransformation} on which
 * operator state is partitioned by key using a provided {@link KeySelector}.
 *
 * @param <K> The type of the key in the Keyed OperatorTransformation.
 * @param <T> The type of the elements in the Keyed OperatorTransformation.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public class KeyedStateTransformation<K, T> {

    /** The data set containing the data to bootstrap the operator state with. */
    private final DataStream<T> stream;

    /** Local max parallelism for the bootstrapped operator. */
    private final OptionalInt operatorMaxParallelism;

    /** Partitioner for the bootstrapping data set. */
    private final KeySelector<T, K> keySelector;

    /** Type information for the key of the bootstrapped operator. */
    private final TypeInformation<K> keyType;

    KeyedStateTransformation(
            DataStream<T> stream,
            OptionalInt operatorMaxParallelism,
            KeySelector<T, K> keySelector,
            TypeInformation<K> keyType) {
        this.stream = stream;
        this.operatorMaxParallelism = operatorMaxParallelism;
        this.keySelector = keySelector;
        this.keyType = keyType;
    }

    /**
     * Applies the given {@link KeyedStateBootstrapFunction} on the keyed input.
     *
     * <p>The function will be called for every element in the input and can be used for writing
     * both keyed and operator state into a {@link Savepoint}.
     *
     * @param processFunction The {@link KeyedStateBootstrapFunction} that is called for each
     *     element.
     * @return An {@link StateBootstrapTransformation} that can be added to a {@link Savepoint}.
     */
    public StateBootstrapTransformation<T> transform(
            KeyedStateBootstrapFunction<K, T> processFunction) {
        SavepointWriterOperatorFactory factory =
                (timestamp, path) ->
                        new KeyedStateBootstrapOperator<>(timestamp, path, processFunction);
        return transform(factory);
    }

    /**
     * Method for passing user defined operators along with the type information that will transform
     * the OperatorTransformation.
     *
     * <p><b>IMPORTANT:</b> Any output from this operator will be discarded.
     *
     * @param factory A factory returning transformation logic type of the return stream
     * @return An {@link StateBootstrapTransformation} that can be added to a {@link Savepoint}.
     */
    public StateBootstrapTransformation<T> transform(SavepointWriterOperatorFactory factory) {
        return new StateBootstrapTransformation<>(
                stream, operatorMaxParallelism, factory, keySelector, keyType);
    }

    /**
     * Windows this transformation into a {@code WindowedOperatorTransformation}, which bootstraps
     * state that can be restored by a {@code WindowOperator}. Elements are put into windows by a
     * {@link WindowAssigner}. The grouping of elements is done both by key and by window.
     *
     * <p>A {@link org.apache.flink.streaming.api.windowing.triggers.Trigger} can be defined to
     * specify when windows are evaluated. However, {@code WindowAssigners} have a default {@code
     * Trigger} that is used if a {@code Trigger} is not specified.
     *
     * @param assigner The {@code WindowAssigner} that assigns elements to windows.
     */
    public <W extends Window> WindowedStateTransformation<T, K, W> window(
            WindowAssigner<? super T, W> assigner) {
        return new WindowedStateTransformation<>(
                stream, operatorMaxParallelism, keySelector, keyType, assigner);
    }
}
