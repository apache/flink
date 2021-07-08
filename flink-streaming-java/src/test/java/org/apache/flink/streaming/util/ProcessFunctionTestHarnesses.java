/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.api.operators.co.CoProcessOperator;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/** Utility class facilitating creation of test harnesses for various kinds of process functions. */
public class ProcessFunctionTestHarnesses {

    /**
     * Returns an initialized test harness for {@link ProcessFunction}.
     *
     * @param function instance of a {@link ProcessFunction} under test
     * @param <IN> type of input stream elements
     * @param <OUT> type of output stream elements
     * @return {@link OneInputStreamOperatorTestHarness} wrapped around {@code function}
     */
    public static <IN, OUT> OneInputStreamOperatorTestHarness<IN, OUT> forProcessFunction(
            final ProcessFunction<IN, OUT> function) throws Exception {

        OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new ProcessOperator<>(Preconditions.checkNotNull(function)), 1, 1, 0);
        testHarness.setup();
        testHarness.open();
        return testHarness;
    }

    /**
     * Returns an initialized test harness for {@link KeyedProcessFunction}.
     *
     * @param function instance of a {@link KeyedCoProcessFunction} under test
     * @param <K> key type
     * @param <IN> type of input stream elements
     * @param <OUT> type of output stream elements
     * @return {@link KeyedOneInputStreamOperatorTestHarness} wrapped around {@code function}
     */
    public static <K, IN, OUT>
            KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> forKeyedProcessFunction(
                    final KeyedProcessFunction<K, IN, OUT> function,
                    final KeySelector<IN, K> keySelector,
                    final TypeInformation<K> keyType)
                    throws Exception {

        KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(Preconditions.checkNotNull(function)),
                        keySelector,
                        keyType,
                        1,
                        1,
                        0);
        testHarness.open();
        return testHarness;
    }

    /**
     * Returns an initialized test harness for {@link CoProcessFunction} with two input streams.
     *
     * @param function instance of a {@link CoProcessFunction} under test
     * @param <IN1> type of first input stream elements
     * @param <IN2> type of second input stream elements
     * @param <OUT> type of output stream elements
     * @return {@link TwoInputStreamOperatorTestHarness} wrapped around {@code function}
     */
    public static <IN1, IN2, OUT>
            TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> forCoProcessFunction(
                    final CoProcessFunction<IN1, IN2, OUT> function) throws Exception {

        TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> testHarness =
                new TwoInputStreamOperatorTestHarness<>(
                        new CoProcessOperator<>(Preconditions.checkNotNull(function)), 1, 1, 0);
        testHarness.open();
        return testHarness;
    }

    /**
     * Returns an initialized test harness for {@link KeyedCoProcessFunction} with two input
     * streams.
     *
     * @param function instance of a {@link KeyedCoProcessFunction} under test
     * @param <K> key type
     * @param <IN1> type of first input stream elements
     * @param <IN2> type of second input stream elements
     * @param <OUT> type of output stream elements
     * @return {@link KeyedOneInputStreamOperatorTestHarness} wrapped around {@code function}
     */
    public static <K, IN1, IN2, OUT>
            KeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT> forKeyedCoProcessFunction(
                    final KeyedCoProcessFunction<K, IN1, IN2, OUT> function,
                    final KeySelector<IN1, K> keySelector1,
                    final KeySelector<IN2, K> keySelector2,
                    final TypeInformation<K> keyType)
                    throws Exception {

        KeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new KeyedCoProcessOperator<>(Preconditions.checkNotNull(function)),
                        keySelector1,
                        keySelector2,
                        keyType,
                        1,
                        1,
                        0);

        testHarness.open();
        return testHarness;
    }

    /**
     * Returns an initialized test harness for {@link BroadcastProcessFunction}.
     *
     * @param function instance of a {@link BroadcastProcessFunction} under test
     * @param descriptors broadcast state descriptors used in the {@code function}
     * @param <IN1> type of input stream elements
     * @param <IN2> type of broadcast stream elements
     * @param <OUT> type of output elements
     * @return {@link BroadcastOperatorTestHarness} wrapped around {@code function}
     */
    public static <IN1, IN2, OUT>
            BroadcastOperatorTestHarness<IN1, IN2, OUT> forBroadcastProcessFunction(
                    final BroadcastProcessFunction<IN1, IN2, OUT> function,
                    final MapStateDescriptor<?, ?>... descriptors)
                    throws Exception {

        BroadcastOperatorTestHarness<IN1, IN2, OUT> testHarness =
                new BroadcastOperatorTestHarness<>(
                        new CoBroadcastWithNonKeyedOperator<>(
                                Preconditions.checkNotNull(function), Arrays.asList(descriptors)),
                        1,
                        1,
                        0);
        testHarness.open();
        return testHarness;
    }

    /**
     * Returns an initialized test harness for {@link KeyedBroadcastProcessFunction}.
     *
     * @param function instance of a {@link KeyedBroadcastProcessFunction} under test
     * @param descriptors broadcast state descriptors used in the {@code function}
     * @param <K> key type
     * @param <IN1> type of input stream elements
     * @param <IN2> type of broadcast stream elements
     * @param <OUT> type of output elements
     * @return {@link BroadcastOperatorTestHarness} wrapped around {@code function}
     */
    public static <K, IN1, IN2, OUT>
            KeyedBroadcastOperatorTestHarness<K, IN1, IN2, OUT> forKeyedBroadcastProcessFunction(
                    final KeyedBroadcastProcessFunction<K, IN1, IN2, OUT> function,
                    final KeySelector<IN1, K> keySelector,
                    final TypeInformation<K> keyType,
                    final MapStateDescriptor<?, ?>... descriptors)
                    throws Exception {

        KeyedBroadcastOperatorTestHarness<K, IN1, IN2, OUT> testHarness =
                new KeyedBroadcastOperatorTestHarness<>(
                        new CoBroadcastWithKeyedOperator<>(
                                Preconditions.checkNotNull(function), Arrays.asList(descriptors)),
                        keySelector,
                        keyType,
                        1,
                        1,
                        0);

        testHarness.open();
        return testHarness;
    }
}
