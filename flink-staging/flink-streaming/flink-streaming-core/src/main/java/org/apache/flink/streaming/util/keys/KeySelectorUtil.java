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

import java.lang.reflect.Array;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 * Utility class that contains helper methods to manipulating {@link KeySelector} for streaming.
 */
public final class KeySelectorUtil {

	public static <X> KeySelector<X, Tuple> getSelectorForKeys(Keys<X> keys, TypeInformation<X> typeInfo, ExecutionConfig executionConfig) {
		if (!(typeInfo instanceof CompositeType)) {
			throw new InvalidTypesException(
					"This key operation requires a composite type such as Tuples, POJOs, or Case Classes.");
		}

		CompositeType<X> compositeType = (CompositeType<X>) typeInfo;
		
		int[] logicalKeyPositions = keys.computeLogicalKeyPositions();
		int numKeyFields = logicalKeyPositions.length;
		
		// use ascending order here, the code paths for that are usually a slight bit faster
		boolean[] orders = new boolean[numKeyFields];
		TypeInformation[] typeInfos = new TypeInformation[numKeyFields];
		for (int i = 0; i < numKeyFields; i++) {
			orders[i] = true;
			typeInfos[i] = compositeType.getTypeAt(logicalKeyPositions[i]);
		}

		TypeComparator<X> comparator = compositeType.createComparator(logicalKeyPositions, orders, 0, executionConfig);
		return new ComparableKeySelector<>(comparator, numKeyFields, new TupleTypeInfo<>(typeInfos));
	}

	
	public static <X, K> KeySelector<X, K> getSelectorForOneKey(Keys<X> keys, Partitioner<K> partitioner, TypeInformation<X> typeInfo,
			ExecutionConfig executionConfig) {
		if (partitioner != null) {
			keys.validateCustomPartitioner(partitioner, null);
		}

		int[] logicalKeyPositions = keys.computeLogicalKeyPositions();

		if (logicalKeyPositions.length != 1) {
			throw new IllegalArgumentException("There must be exactly 1 key specified");
		}

		TypeComparator<X> comparator = ((CompositeType<X>) typeInfo).createComparator(
				logicalKeyPositions, new boolean[1], 0, executionConfig);
		return new OneKeySelector<>(comparator);
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private KeySelectorUtil() {
		throw new RuntimeException();
	}
	
	public static final class OneKeySelector<IN, K> implements KeySelector<IN, K> {

		private static final long serialVersionUID = 1L;

		private final TypeComparator<IN> comparator;

		/** Reusable array to hold the key objects. Since this is initially empty (all positions
		 * are null), it does not have any serialization problems */
		@SuppressWarnings("NonSerializableFieldInSerializableClass")
		private final Object[] keyArray;

		public OneKeySelector(TypeComparator<IN> comparator) {
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
	public static final class ComparableKeySelector<IN> implements KeySelector<IN, Tuple>, ResultTypeQueryable<Tuple> {

		private static final long serialVersionUID = 1L;

		private final TypeComparator<IN> comparator;
		private final int keyLength;
		private final TupleTypeInfo tupleTypeInfo;

		/** Reusable array to hold the key objects. Since this is initially empty (all positions
		 * are null), it does not have any serialization problems */
		@SuppressWarnings("NonSerializableFieldInSerializableClass")
		private final Object[] keyArray;

		public ComparableKeySelector(TypeComparator<IN> comparator, int keyLength, TupleTypeInfo tupleTypeInfo) {
			this.comparator = comparator;
			this.keyLength = keyLength;
			this.tupleTypeInfo = tupleTypeInfo;
			keyArray = new Object[keyLength];
		}

		@Override
		public Tuple getKey(IN value) throws Exception {
			Tuple key = Tuple.getTupleClass(keyLength).newInstance();
			comparator.extractKeys(value, keyArray, 0);
			for (int i = 0; i < keyLength; i++) {
				key.setField(keyArray[i], i);
			}
			return key;
		}

		@Override
		public TypeInformation<Tuple> getProducedType() {
			return tupleTypeInfo;
		}
	}

	// ------------------------------------------------------------------------
	
	/**
	 * A key selector for selecting individual array fields as keys and returns them as a Tuple.
	 * 
	 * @param <IN> The type from which the key is extracted, i.e., the array type.
	 */
	public static final class ArrayKeySelector<IN> implements KeySelector<IN, Tuple> {

		private static final long serialVersionUID = 1L;
		
		private final int[] fields;

		public ArrayKeySelector(int... fields) {
			this.fields = fields;
		}

		@Override
		public Tuple getKey(IN value) throws Exception {
			Tuple key = Tuple.getTupleClass(fields.length).newInstance();
			for (int i = 0; i < fields.length; i++) {
				key.setField(Array.get(value, fields[i]), i);
			}
			return key;
		}
	}
}
