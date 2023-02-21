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
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.state.api.functions.BroadcastStateBootstrapFunction;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.state.api.output.operators.BroadcastStateBootstrapOperator;
import org.apache.flink.state.api.output.operators.StateBootstrapOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;

import java.util.OptionalInt;

/**
 * {@code OneInputStateTransformation} represents a user defined transformation applied on an {@link
 * OperatorTransformation} with one input.
 *
 * @param <T> The type of the elements in this operator.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public class OneInputStateTransformation<T> {

    /** The data stream containing the data to bootstrap the operator state with. */
    private final DataStream<T> stream;

    /** Local max parallelism for the bootstrapped operator. */
    private OptionalInt operatorMaxParallelism = OptionalInt.empty();

    OneInputStateTransformation(DataStream<T> stream) {
        this.stream = stream;
    }

    /**
     * Sets the maximum parallelism of this operator.
     *
     * <p>The maximum parallelism specifies the upper bound for dynamic scaling. It also defines the
     * number of key groups used for partitioned state.
     *
     * @param maxParallelism Maximum parallelism
     * @return The operator with set maximum parallelism
     */
    @PublicEvolving
    public OneInputStateTransformation<T> setMaxParallelism(int maxParallelism) {
        this.operatorMaxParallelism = OptionalInt.of(maxParallelism);
        return this;
    }

    /**
     * Applies the given {@link StateBootstrapFunction} on the non-keyed input.
     *
     * <p>The function will be called for every element in the input and can be used for writing
     * operator state into a {@link Savepoint}.
     *
     * @param processFunction The {@link StateBootstrapFunction} that is called for each element.
     * @return An {@link OperatorTransformation} that can be added to a {@link Savepoint}.
     */
    public StateBootstrapTransformation<T> transform(StateBootstrapFunction<T> processFunction) {
        SavepointWriterOperatorFactory factory =
                (timestamp, path) -> new StateBootstrapOperator<>(timestamp, path, processFunction);

        return transform(factory);
    }

    /**
     * Applies the given {@link BroadcastStateBootstrapFunction} on the non-keyed input.
     *
     * <p>The function will be called for every element in the input and can be used for writing
     * broadcast state into a {@link Savepoint}.
     *
     * @param processFunction The {@link BroadcastStateBootstrapFunction} that is called for each
     *     element.
     * @return An {@link StateBootstrapTransformation} that can be added to a {@link Savepoint}.
     */
    public StateBootstrapTransformation<T> transform(
            BroadcastStateBootstrapFunction<T> processFunction) {
        SavepointWriterOperatorFactory factory =
                (timestamp, path) ->
                        new BroadcastStateBootstrapOperator<>(timestamp, path, processFunction);

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
        return new StateBootstrapTransformation<>(stream, operatorMaxParallelism, factory);
    }

    /**
     * It creates a new {@link KeyedOperatorTransformation} that uses the provided key for
     * partitioning its operator states.
     *
     * @param keySelector The KeySelector to be used for extracting the key for partitioning.
     * @return The {@code BootstrapTransformation} with partitioned state.
     */
    public <K> KeyedStateTransformation<K, T> keyBy(KeySelector<T, K> keySelector) {
        TypeInformation<K> keyType =
                TypeExtractor.getKeySelectorTypes(keySelector, stream.getType());
        return new KeyedStateTransformation<>(stream, operatorMaxParallelism, keySelector, keyType);
    }

    /**
     * It creates a new {@link KeyedOperatorTransformation} that uses the provided key with explicit
     * type information for partitioning its operator states.
     *
     * @param keySelector The KeySelector to be used for extracting the key for partitioning.
     * @param keyType The type information describing the key type.
     * @return The {@code BootstrapTransformation} with partitioned state.
     */
    public <K> KeyedStateTransformation<K, T> keyBy(
            KeySelector<T, K> keySelector, TypeInformation<K> keyType) {
        return new KeyedStateTransformation<>(stream, operatorMaxParallelism, keySelector, keyType);
    }

    /**
     * Partitions the operator state of a {@link OperatorTransformation} by the given key positions.
     *
     * @param fields The position of the fields on which the {@code OperatorTransformation} will be
     *     grouped.
     * @return The {@code OperatorTransformation} with partitioned state.
     */
    public KeyedStateTransformation<Tuple, T> keyBy(int... fields) {
        if (stream.getType() instanceof BasicArrayTypeInfo
                || stream.getType() instanceof PrimitiveArrayTypeInfo) {
            return keyBy(KeySelectorUtil.getSelectorForArray(fields, stream.getType()));
        } else {
            return keyBy(new Keys.ExpressionKeys<>(fields, stream.getType()));
        }
    }

    /**
     * Partitions the operator state of a {@link OperatorTransformation} using field expressions. A
     * field expression is either the name of a public field or a getter method with parentheses of
     * the {@code OperatorTransformation}'s underlying type. A dot can be used to drill down into
     * objects, as in {@code "field1.getInnerField2()" }.
     *
     * @param fields One or more field expressions on which the state of the {@link
     *     OperatorTransformation} operators will be partitioned.
     * @return The {@code OperatorTransformation} with partitioned state (i.e. KeyedStream)
     */
    public KeyedStateTransformation<Tuple, T> keyBy(String... fields) {
        return keyBy(new Keys.ExpressionKeys<>(fields, stream.getType()));
    }

    private KeyedStateTransformation<Tuple, T> keyBy(Keys<T> keys) {
        KeySelector<T, Tuple> keySelector =
                KeySelectorUtil.getSelectorForKeys(
                        keys, stream.getType(), stream.getExecutionEnvironment().getConfig());

        TypeInformation<Tuple> keyType =
                TypeExtractor.getKeySelectorTypes(keySelector, stream.getType());
        return new KeyedStateTransformation<>(stream, operatorMaxParallelism, keySelector, keyType);
    }
}
