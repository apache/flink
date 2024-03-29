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

package org.apache.flink.datastream.impl.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.stream.AbstractDataStream;
import org.apache.flink.datastream.impl.stream.KeyedPartitionStreamImpl;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

/**
 * This class encapsulates the common logic for all type of streams. It can be used to handle things
 * like extract type information, create a new transformation and so on for AbstractDataStream.
 */
public final class StreamUtils {
    /**
     * Get the output type information for {@link OneInputStreamProcessFunction} from input type
     * information.
     */
    public static <IN, OUT> TypeInformation<OUT> getOutputTypeForOneInputProcessFunction(
            OneInputStreamProcessFunction<IN, OUT> processFunction,
            TypeInformation<IN> inTypeInformation) {
        return TypeExtractor.getUnaryOperatorReturnType(
                processFunction,
                OneInputStreamProcessFunction.class,
                0,
                1,
                new int[] {1, 0},
                inTypeInformation,
                Utils.getCallLocationName(),
                true);
    }

    /**
     * Get the output type information for {@link TwoInputNonBroadcastStreamProcessFunction} from
     * two input type information .
     */
    public static <IN1, IN2, OUT>
            TypeInformation<OUT> getOutputTypeForTwoInputNonBroadcastProcessFunction(
                    TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> processFunction,
                    TypeInformation<IN1> in1TypeInformation,
                    TypeInformation<IN2> in2TypeInformation) {
        return TypeExtractor.getBinaryOperatorReturnType(
                processFunction,
                TwoInputNonBroadcastStreamProcessFunction.class,
                0,
                1,
                2,
                TypeExtractor.NO_INDEX,
                in1TypeInformation,
                in2TypeInformation,
                Utils.getCallLocationName(),
                true);
    }

    /**
     * Get the output type information for {@link TwoInputBroadcastStreamProcessFunction} from two
     * input type information .
     */
    public static <IN1, IN2, OUT>
            TypeInformation<OUT> getOutputTypeForTwoInputBroadcastProcessFunction(
                    TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT> processFunction,
                    TypeInformation<IN1> in1TypeInformation,
                    TypeInformation<IN2> in2TypeInformation) {
        return TypeExtractor.getBinaryOperatorReturnType(
                processFunction,
                TwoInputBroadcastStreamProcessFunction.class,
                0,
                1,
                2,
                TypeExtractor.NO_INDEX,
                in1TypeInformation,
                in2TypeInformation,
                Utils.getCallLocationName(),
                true);
    }

    /**
     * Get output types information for {@link TwoOutputStreamProcessFunction} from the input type
     * information.
     */
    public static <IN, OUT1, OUT2>
            Tuple2<TypeInformation<OUT1>, TypeInformation<OUT2>>
                    getOutputTypesForTwoOutputProcessFunction(
                            TwoOutputStreamProcessFunction<IN, OUT1, OUT2>
                                    twoOutputStreamProcessFunction,
                            TypeInformation<IN> inTypeInformation) {
        TypeInformation<OUT1> firstOutputType =
                TypeExtractor.getUnaryOperatorReturnType(
                        twoOutputStreamProcessFunction,
                        TwoOutputStreamProcessFunction.class,
                        0,
                        1,
                        new int[] {1, 0},
                        inTypeInformation,
                        Utils.getCallLocationName(),
                        true);
        TypeInformation<OUT2> secondOutputType =
                TypeExtractor.getUnaryOperatorReturnType(
                        twoOutputStreamProcessFunction,
                        TwoOutputStreamProcessFunction.class,
                        0,
                        2,
                        new int[] {2, 0},
                        inTypeInformation,
                        Utils.getCallLocationName(),
                        true);
        return Tuple2.of(firstOutputType, secondOutputType);
    }

    /** Construct and return a {@link OneInputTransformation} from non-keyed input streams. */
    public static <T, R> OneInputTransformation<T, R> getOneInputTransformation(
            String operatorName,
            AbstractDataStream<T> inputStream,
            TypeInformation<R> outTypeInformation,
            OneInputStreamOperator<T, R> operator) {
        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        inputStream.getTransformation().getOutputType();

        OneInputTransformation<T, R> resultTransform =
                new OneInputTransformation<>(
                        inputStream.getTransformation(),
                        operatorName,
                        SimpleUdfStreamOperatorFactory.of(operator),
                        outTypeInformation,
                        inputStream.getEnvironment().getParallelism(),
                        false);

        return resultTransform;
    }

    /** Construct and return a {@link OneInputTransformation} from keyed input streams. */
    public static <T, R, K> OneInputTransformation<T, R> getOneInputKeyedTransformation(
            String operatorName,
            AbstractDataStream<T> inputStream,
            TypeInformation<R> outTypeInformation,
            OneInputStreamOperator<T, R> operator,
            KeySelector<T, K> keySelector,
            TypeInformation<K> keyType) {
        OneInputTransformation<T, R> resultTransform =
                getOneInputTransformation(operatorName, inputStream, outTypeInformation, operator);

        // inject the key selector and key type
        resultTransform.setStateKeySelector(keySelector);
        resultTransform.setStateKeyType(keyType);

        return resultTransform;
    }

    /** Construct and return a {@link TwoInputTransformation} from two input streams. */
    public static <IN1, IN2, OUT> TwoInputTransformation<IN1, IN2, OUT> getTwoInputTransformation(
            String operatorName,
            AbstractDataStream<IN1> inputStream1,
            AbstractDataStream<IN2> inputStream2,
            TypeInformation<OUT> outTypeInformation,
            TwoInputStreamOperator<IN1, IN2, OUT> operator) {
        TwoInputTransformation<IN1, IN2, OUT> transform =
                new TwoInputTransformation<>(
                        inputStream1.getTransformation(),
                        inputStream2.getTransformation(),
                        operatorName,
                        SimpleOperatorFactory.of(operator),
                        outTypeInformation,
                        // inputStream1 & 2 share the same env.
                        inputStream1.getEnvironment().getParallelism(),
                        false);

        TypeInformation<?> keyType = null;
        if (inputStream1 instanceof KeyedPartitionStreamImpl) {
            KeyedPartitionStreamImpl<?, IN1> keyedInput1 =
                    (KeyedPartitionStreamImpl<?, IN1>) inputStream1;

            keyType = keyedInput1.getKeyType();

            transform.setStateKeySelectors(keyedInput1.getKeySelector(), null);
            transform.setStateKeyType(keyType);
        }
        if (inputStream2 instanceof KeyedPartitionStreamImpl) {
            KeyedPartitionStreamImpl<?, IN2> keyedInput2 =
                    (KeyedPartitionStreamImpl<?, IN2>) inputStream2;

            TypeInformation<?> keyType2 = keyedInput2.getKeyType();

            if (keyType != null && !(keyType.canEqual(keyType2) && keyType.equals(keyType2))) {
                throw new UnsupportedOperationException(
                        "Key types if input KeyedStreams "
                                + "don't match: "
                                + keyType
                                + " and "
                                + keyType2
                                + ".");
            }

            transform.setStateKeySelectors(
                    transform.getStateKeySelector1(), keyedInput2.getKeySelector());

            // we might be overwriting the one that's already set, but it's the same
            transform.setStateKeyType(keyType2);
        }

        return transform;
    }
}
