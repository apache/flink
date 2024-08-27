/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.keys;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.lang.reflect.Array;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/** Utility class that contains helper methods to manipulating {@link KeySelector} for streaming. */
@Internal
public final class KeySelectorUtil {

    public static <X> KeySelector<X, Tuple> getSelectorForKeys(
            Keys<X> keys, TypeInformation<X> typeInfo, ExecutionConfig executionConfig) {
        if (!(typeInfo instanceof CompositeType)) {
            throw new InvalidTypesException(
                    "This key operation requires a composite type such as Tuples, POJOs, or Case Classes.");
        }

        CompositeType<X> compositeType = (CompositeType<X>) typeInfo;

        int[] logicalKeyPositions = keys.computeLogicalKeyPositions();
        int numKeyFields = logicalKeyPositions.length;

        TypeInformation<?>[] typeInfos = keys.getKeyFieldTypes();
        // use ascending order here, the code paths for that are usually a slight bit faster
        boolean[] orders = new boolean[numKeyFields];
        for (int i = 0; i < numKeyFields; i++) {
            orders[i] = true;
        }

        TypeComparator<X> comparator =
                compositeType.createComparator(logicalKeyPositions, orders, 0, executionConfig);
        return new ComparableKeySelector<>(
                comparator, numKeyFields, new TupleTypeInfo<>(typeInfos));
    }

    public static <X> ArrayKeySelector<X> getSelectorForArray(
            int[] positions, TypeInformation<X> typeInfo) {
        if (positions == null || positions.length == 0 || positions.length > Tuple.MAX_ARITY) {
            throw new IllegalArgumentException(
                    "Array keys must have between 1 and " + Tuple.MAX_ARITY + " fields.");
        }

        TypeInformation<?> componentType;

        if (typeInfo instanceof BasicArrayTypeInfo) {
            BasicArrayTypeInfo<X, ?> arrayInfo = (BasicArrayTypeInfo<X, ?>) typeInfo;
            componentType = arrayInfo.getComponentInfo();
        } else if (typeInfo instanceof PrimitiveArrayTypeInfo) {
            PrimitiveArrayTypeInfo<X> arrayType = (PrimitiveArrayTypeInfo<X>) typeInfo;
            componentType = arrayType.getComponentType();
        } else {
            throw new IllegalArgumentException(
                    "This method only supports arrays of primitives and boxed primitives.");
        }

        TypeInformation<?>[] primitiveInfos = new TypeInformation<?>[positions.length];
        Arrays.fill(primitiveInfos, componentType);

        return new ArrayKeySelector<>(positions, new TupleTypeInfo<>(primitiveInfos));
    }

    public static <X, K> KeySelector<X, K> getSelectorForOneKey(
            Keys<X> keys,
            Partitioner<K> partitioner,
            TypeInformation<X> typeInfo,
            ExecutionConfig executionConfig) {
        if (!(typeInfo instanceof CompositeType)) {
            throw new InvalidTypesException(
                    "This key operation requires a composite type such as Tuples, POJOs, case classes, etc");
        }
        if (partitioner != null) {
            keys.validateCustomPartitioner(partitioner, null);
        }

        CompositeType<X> compositeType = (CompositeType<X>) typeInfo;
        int[] logicalKeyPositions = keys.computeLogicalKeyPositions();
        if (logicalKeyPositions.length != 1) {
            throw new IllegalArgumentException("There must be exactly 1 key specified");
        }

        TypeComparator<X> comparator =
                compositeType.createComparator(
                        logicalKeyPositions, new boolean[] {true}, 0, executionConfig);
        return new OneKeySelector<>(comparator);
    }

    // ------------------------------------------------------------------------

    /** Private constructor to prevent instantiation. */
    private KeySelectorUtil() {
        throw new RuntimeException();
    }

    // ------------------------------------------------------------------------

    /**
     * Key extractor that extracts a single field via a generic comparator.
     *
     * @param <IN> The type of the elements where the key is extracted from.
     * @param <K> The type of the key.
     */
    public static final class OneKeySelector<IN, K> implements KeySelector<IN, K> {

        private static final long serialVersionUID = 1L;

        private final TypeComparator<IN> comparator;

        /**
         * Reusable array to hold the key objects. Since this is initially empty (all positions are
         * null), it does not have any serialization problems
         */
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Object[] keyArray;

        OneKeySelector(TypeComparator<IN> comparator) {
            this.comparator = comparator;
            this.keyArray = new Object[1];
        }

        @Override
        @SuppressWarnings("unchecked")
        public K getKey(IN value) throws Exception {
            comparator.extractKeys(value, keyArray, 0);
            return (K) keyArray[0];
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A key selector for selecting key fields via a TypeComparator.
     *
     * @param <IN> The type from which the key is extracted.
     */
    public static final class ComparableKeySelector<IN>
            implements KeySelector<IN, Tuple>, ResultTypeQueryable<Tuple> {

        private static final long serialVersionUID = 1L;

        private final TypeComparator<IN> comparator;
        private final int keyLength;
        private transient TupleTypeInfo<Tuple> tupleTypeInfo;

        /**
         * Reusable array to hold the key objects. Since this is initially empty (all positions are
         * null), it does not have any serialization problems
         */
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Object[] keyArray;

        ComparableKeySelector(
                TypeComparator<IN> comparator, int keyLength, TupleTypeInfo<Tuple> tupleTypeInfo) {
            this.comparator = comparator;
            this.keyLength = keyLength;
            this.tupleTypeInfo = tupleTypeInfo;
            this.keyArray = new Object[keyLength];
        }

        @Override
        public Tuple getKey(IN value) {
            Tuple key = Tuple.newInstance(keyLength);
            comparator.extractKeys(value, keyArray, 0);
            for (int i = 0; i < keyLength; i++) {
                key.setField(keyArray[i], i);
            }
            return key;
        }

        @Override
        public TypeInformation<Tuple> getProducedType() {
            if (tupleTypeInfo == null) {
                throw new IllegalStateException(
                        "The return type information is not available after serialization");
            }
            return tupleTypeInfo;
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A key selector for selecting individual array fields as keys and returns them as a Tuple.
     *
     * @param <IN> The type from which the key is extracted, i.e., the array type.
     */
    public static final class ArrayKeySelector<IN>
            implements KeySelector<IN, Tuple>, ResultTypeQueryable<Tuple> {

        private static final long serialVersionUID = 1L;

        private final int[] fields;
        private transient TupleTypeInfo<Tuple> returnType;

        ArrayKeySelector(int[] fields, TupleTypeInfo<Tuple> returnType) {
            this.fields = requireNonNull(fields);
            this.returnType = requireNonNull(returnType);
        }

        @Override
        public Tuple getKey(IN value) {
            Tuple key = Tuple.newInstance(fields.length);
            for (int i = 0; i < fields.length; i++) {
                key.setField(Array.get(value, fields[i]), i);
            }
            return key;
        }

        @Override
        public TypeInformation<Tuple> getProducedType() {
            if (returnType == null) {
                throw new IllegalStateException(
                        "The return type information is not available after serialization");
            }
            return returnType;
        }
    }
}
