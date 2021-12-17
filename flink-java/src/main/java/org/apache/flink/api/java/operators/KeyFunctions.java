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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Keys.SelectorFunctionKeys;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.Union;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.KeyRemovingMapper;
import org.apache.flink.api.java.operators.translation.TwoKeyExtractingMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/** This class holds static utilities to append functions that extract and prune keys. */
@Internal
public class KeyFunctions {

    @SuppressWarnings("unchecked")
    public static <T, K>
            org.apache.flink.api.common.operators.Operator<Tuple2<K, T>> appendKeyExtractor(
                    org.apache.flink.api.common.operators.Operator<T> input,
                    SelectorFunctionKeys<T, K> key) {

        if (input instanceof Union) {
            // if input is a union, we apply the key extractors recursively to all inputs
            org.apache.flink.api.common.operators.Operator<T> firstInput =
                    ((Union) input).getFirstInput();
            org.apache.flink.api.common.operators.Operator<T> secondInput =
                    ((Union) input).getSecondInput();

            org.apache.flink.api.common.operators.Operator<Tuple2<K, T>> firstInputWithKey =
                    appendKeyExtractor(firstInput, key);
            org.apache.flink.api.common.operators.Operator<Tuple2<K, T>> secondInputWithKey =
                    appendKeyExtractor(secondInput, key);

            return new Union(firstInputWithKey, secondInputWithKey, input.getName());
        }

        TypeInformation<T> inputType = key.getInputType();
        TypeInformation<Tuple2<K, T>> typeInfoWithKey = createTypeWithKey(key);
        KeyExtractingMapper<T, K> extractor = new KeyExtractingMapper(key.getKeyExtractor());

        MapOperatorBase<T, Tuple2<K, T>, MapFunction<T, Tuple2<K, T>>> mapper =
                new MapOperatorBase<T, Tuple2<K, T>, MapFunction<T, Tuple2<K, T>>>(
                        extractor,
                        new UnaryOperatorInformation(inputType, typeInfoWithKey),
                        "Key Extractor");

        mapper.setInput(input);
        mapper.setParallelism(input.getParallelism());

        return mapper;
    }

    @SuppressWarnings("unchecked")
    public static <T, K1, K2>
            org.apache.flink.api.common.operators.Operator<Tuple3<K1, K2, T>> appendKeyExtractor(
                    org.apache.flink.api.common.operators.Operator<T> input,
                    SelectorFunctionKeys<T, K1> key1,
                    SelectorFunctionKeys<T, K2> key2) {

        if (input instanceof Union) {
            // if input is a union, we apply the key extractors recursively to all inputs
            org.apache.flink.api.common.operators.Operator<T> firstInput =
                    ((Union) input).getFirstInput();
            org.apache.flink.api.common.operators.Operator<T> secondInput =
                    ((Union) input).getSecondInput();

            org.apache.flink.api.common.operators.Operator<Tuple3<K1, K2, T>> firstInputWithKey =
                    appendKeyExtractor(firstInput, key1, key2);
            org.apache.flink.api.common.operators.Operator<Tuple3<K1, K2, T>> secondInputWithKey =
                    appendKeyExtractor(secondInput, key1, key2);

            return new Union(firstInputWithKey, secondInputWithKey, input.getName());
        }

        TypeInformation<T> inputType = key1.getInputType();
        TypeInformation<Tuple3<K1, K2, T>> typeInfoWithKey = createTypeWithKey(key1, key2);
        TwoKeyExtractingMapper<T, K1, K2> extractor =
                new TwoKeyExtractingMapper<>(key1.getKeyExtractor(), key2.getKeyExtractor());

        MapOperatorBase<T, Tuple3<K1, K2, T>, MapFunction<T, Tuple3<K1, K2, T>>> mapper =
                new MapOperatorBase<T, Tuple3<K1, K2, T>, MapFunction<T, Tuple3<K1, K2, T>>>(
                        extractor,
                        new UnaryOperatorInformation<>(inputType, typeInfoWithKey),
                        "Key Extractor");

        mapper.setInput(input);
        mapper.setParallelism(input.getParallelism());

        return mapper;
    }

    public static <T, K>
            org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?> appendKeyRemover(
                    org.apache.flink.api.common.operators.Operator<Tuple2<K, T>> inputWithKey,
                    SelectorFunctionKeys<T, K> key) {

        TypeInformation<T> inputType = key.getInputType();
        TypeInformation<Tuple2<K, T>> typeInfoWithKey = createTypeWithKey(key);

        MapOperatorBase<Tuple2<K, T>, T, MapFunction<Tuple2<K, T>, T>> mapper =
                new MapOperatorBase<Tuple2<K, T>, T, MapFunction<Tuple2<K, T>, T>>(
                        new KeyRemovingMapper<T, K>(),
                        new UnaryOperatorInformation<>(typeInfoWithKey, inputType),
                        "Key Remover");
        mapper.setInput(inputWithKey);
        mapper.setParallelism(inputWithKey.getParallelism());

        return mapper;
    }

    public static <T, K> TypeInformation<Tuple2<K, T>> createTypeWithKey(
            SelectorFunctionKeys<T, K> key) {
        return new TupleTypeInfo<>(key.getKeyType(), key.getInputType());
    }

    public static <T, K1, K2> TypeInformation<Tuple3<K1, K2, T>> createTypeWithKey(
            SelectorFunctionKeys<T, K1> key1, SelectorFunctionKeys<T, K2> key2) {
        return new TupleTypeInfo<>(key1.getKeyType(), key2.getKeyType(), key1.getInputType());
    }
}
