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

package org.apache.flink.datastream.api.builtin;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.join.JoinType;
import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.TwoInputNonBroadcastWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.TwoOutputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;

/** Built-in functions for all extension of datastream v2. */
@Experimental
public class BuiltinFuncs {

    // =================== Join ===========================

    static final Class<?> JOIN_FUNCS_INSTANCE;

    static {
        try {
            JOIN_FUNCS_INSTANCE =
                    Class.forName("org.apache.flink.datastream.impl.builtin.BuiltinJoinFuncs");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Please ensure that flink-datastream in your class path");
        }
    }

    /**
     * Wrap the JoinFunction and INNER JoinType within a ProcessFunction to perform the Join
     * operation. Note that the wrapped process function should only be used with KeyedStream.
     */
    public static <IN1, IN2, OUT> TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> join(
            JoinFunction<IN1, IN2, OUT> joinFunction) {
        return join(joinFunction, JoinType.INNER);
    }

    /**
     * Wrap the JoinFunction and JoinType within a ProcessFunction to perform the Join operation.
     * Note that the wrapped process function should only be used with KeyedStream.
     */
    public static <IN1, IN2, OUT> TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> join(
            JoinFunction<IN1, IN2, OUT> joinFunction, JoinType joinType) {
        try {
            return (TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT>)
                    JOIN_FUNCS_INSTANCE
                            .getMethod("join", JoinFunction.class, JoinType.class)
                            .invoke(null, joinFunction, joinType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Inner join two {@link KeyedPartitionStream}. */
    public static <KEY, T1, T2, OUT> NonKeyedPartitionStream<OUT> join(
            KeyedPartitionStream<KEY, T1> leftStream,
            KeyedPartitionStream<KEY, T2> rightStream,
            JoinFunction<T1, T2, OUT> joinFunction) {
        return join(leftStream, rightStream, joinFunction, JoinType.INNER);
    }

    /** Join two {@link KeyedPartitionStream} with the type of {@link JoinType}. */
    public static <KEY, T1, T2, OUT> NonKeyedPartitionStream<OUT> join(
            KeyedPartitionStream<KEY, T1> leftStream,
            KeyedPartitionStream<KEY, T2> rightStream,
            JoinFunction<T1, T2, OUT> joinFunction,
            JoinType joinType) {
        return leftStream.connectAndProcess(rightStream, join(joinFunction, joinType));
    }

    /**
     * Inner join two {@link NonKeyedPartitionStream}. The two streams will be redistributed by
     * {@link KeySelector} respectively.
     */
    public static <KEY, T1, T2, OUT> NonKeyedPartitionStream<OUT> join(
            NonKeyedPartitionStream<T1> leftStream,
            KeySelector<T1, KEY> leftKeySelector,
            NonKeyedPartitionStream<T2> rightStream,
            KeySelector<T2, KEY> rightKeySelector,
            JoinFunction<T1, T2, OUT> joinFunction) {
        return join(
                leftStream,
                leftKeySelector,
                rightStream,
                rightKeySelector,
                joinFunction,
                JoinType.INNER);
    }

    /**
     * Join two {@link NonKeyedPartitionStream} with the type of {@link JoinType}. The two streams
     * will be redistributed by {@link KeySelector} respectively.
     */
    public static <KEY, T1, T2, OUT> NonKeyedPartitionStream<OUT> join(
            NonKeyedPartitionStream<T1> leftStream,
            KeySelector<T1, KEY> leftKeySelector,
            NonKeyedPartitionStream<T2> rightStream,
            KeySelector<T2, KEY> rightKeySelector,
            JoinFunction<T1, T2, OUT> joinFunction,
            JoinType joinType) {
        return join(
                leftStream.keyBy(leftKeySelector),
                rightStream.keyBy(rightKeySelector),
                joinFunction,
                joinType);
    }

    // =================== Window ===========================

    static final Class<?> WINDOW_FUNCS_INSTANCE;

    static {
        try {
            WINDOW_FUNCS_INSTANCE =
                    Class.forName("org.apache.flink.datastream.impl.builtin.BuiltinWindowFuncs");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Please ensure that flink-datastream in your class path");
        }
    }

    /**
     * Wrap the WindowStrategy and OneInputWindowStreamProcessFunction within a
     * OneInputStreamProcessFunction to perform the window operation.
     *
     * @param windowStrategy the window strategy
     * @param windowProcessFunction the window process function
     * @return the wrapped process function
     */
    public static <IN, OUT> OneInputStreamProcessFunction<IN, OUT> window(
            WindowStrategy windowStrategy,
            OneInputWindowStreamProcessFunction<IN, OUT> windowProcessFunction) {
        try {
            return (OneInputStreamProcessFunction<IN, OUT>)
                    WINDOW_FUNCS_INSTANCE
                            .getMethod(
                                    "window",
                                    WindowStrategy.class,
                                    OneInputWindowStreamProcessFunction.class)
                            .invoke(null, windowStrategy, windowProcessFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Wrap the WindowStrategy and TwoInputNonBroadcastWindowStreamProcessFunction within a
     * TwoInputNonBroadcastStreamProcessFunction to perform the window operation.
     *
     * @param windowStrategy the window strategy
     * @param windowProcessFunction the window process function
     * @return the wrapped process function
     */
    public static <IN1, IN2, OUT> TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> window(
            WindowStrategy windowStrategy,
            TwoInputNonBroadcastWindowStreamProcessFunction<IN1, IN2, OUT> windowProcessFunction) {
        try {
            return (TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT>)
                    WINDOW_FUNCS_INSTANCE
                            .getMethod(
                                    "window",
                                    WindowStrategy.class,
                                    TwoInputNonBroadcastWindowStreamProcessFunction.class)
                            .invoke(null, windowStrategy, windowProcessFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Wrap the WindowStrategy and TwoOutputWindowStreamProcessFunction within a
     * TwoOutputStreamProcessFunction to perform the window operation.
     *
     * @param windowStrategy the window strategy
     * @param windowProcessFunction the window process function
     * @return the wrapped process function
     */
    public static <IN, OUT1, OUT2> TwoOutputStreamProcessFunction<IN, OUT1, OUT2> window(
            WindowStrategy windowStrategy,
            TwoOutputWindowStreamProcessFunction<IN, OUT1, OUT2> windowProcessFunction) {
        try {
            return (TwoOutputStreamProcessFunction<IN, OUT1, OUT2>)
                    WINDOW_FUNCS_INSTANCE
                            .getMethod(
                                    "window",
                                    WindowStrategy.class,
                                    TwoOutputWindowStreamProcessFunction.class)
                            .invoke(null, windowStrategy, windowProcessFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
