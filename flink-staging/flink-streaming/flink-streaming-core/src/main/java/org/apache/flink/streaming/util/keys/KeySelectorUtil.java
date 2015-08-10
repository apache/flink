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
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple20;
import org.apache.flink.api.java.tuple.Tuple21;
import org.apache.flink.api.java.tuple.Tuple22;
import org.apache.flink.api.java.tuple.Tuple23;
import org.apache.flink.api.java.tuple.Tuple24;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;

public class KeySelectorUtil {

	public static Class<?>[] tupleClasses = new Class[] { Tuple1.class, Tuple2.class, Tuple3.class,
			Tuple4.class, Tuple5.class, Tuple6.class, Tuple7.class, Tuple8.class, Tuple9.class,
			Tuple10.class, Tuple11.class, Tuple12.class, Tuple13.class, Tuple14.class,
			Tuple15.class, Tuple16.class, Tuple17.class, Tuple18.class, Tuple19.class,
			Tuple20.class, Tuple21.class, Tuple22.class, Tuple23.class, Tuple24.class,
			Tuple25.class };

	public static <X> KeySelector<X, ?> getSelectorForKeys(Keys<X> keys, TypeInformation<X> typeInfo, ExecutionConfig executionConfig) {
		int[] logicalKeyPositions = keys.computeLogicalKeyPositions();
		int keyLength = logicalKeyPositions.length;
		boolean[] orders = new boolean[keyLength];
		// TODO: Fix using KeySelector everywhere
		TypeComparator<X> comparator = ((CompositeType<X>) typeInfo).createComparator(
				logicalKeyPositions, orders, 0, executionConfig);
		return new ComparableKeySelector<X>(comparator, keyLength);
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
		return new OneKeySelector<X, K>(comparator);
	}

	public static class OneKeySelector<IN, K> implements KeySelector<IN, K> {

		private static final long serialVersionUID = 1L;

		private TypeComparator<IN> comparator;
		private Object[] keyArray;
		private K key;

		public OneKeySelector(TypeComparator<IN> comparator) {
			this.comparator = comparator;
			keyArray = new Object[1];
		}

		@Override
		@SuppressWarnings("unchecked")
		public K getKey(IN value) throws Exception {
			comparator.extractKeys(value, keyArray, 0);
			key = (K) keyArray[0];
			return key;
		}

	}

	public static class ComparableKeySelector<IN> implements KeySelector<IN, Tuple> {

		private static final long serialVersionUID = 1L;

		private TypeComparator<IN> comparator;
		private int keyLength;
		private Object[] keyArray;
		private Tuple key;

		public ComparableKeySelector(TypeComparator<IN> comparator, int keyLength) {
			this.comparator = comparator;
			this.keyLength = keyLength;
			keyArray = new Object[keyLength];
		}

		@Override
		public Tuple getKey(IN value) throws Exception {
			key = (Tuple) tupleClasses[keyLength - 1].newInstance();
			comparator.extractKeys(value, keyArray, 0);
			for (int i = 0; i < keyLength; i++) {
				key.setField(keyArray[i], i);
			}
			return key;
		}

	}

	public static class ArrayKeySelector<IN> implements KeySelector<IN, Tuple> {

		private static final long serialVersionUID = 1L;

		Tuple key;
		int[] fields;

		public ArrayKeySelector(int... fields) {
			this.fields = fields;
		}

		@Override
		public Tuple getKey(IN value) throws Exception {
			key = (Tuple) tupleClasses[fields.length - 1].newInstance();
			for (int i = 0; i < fields.length; i++) {
				int pos = fields[i];
				key.setField(Array.get(value, fields[pos]), i);
			}
			return key;
		}
	}
}
