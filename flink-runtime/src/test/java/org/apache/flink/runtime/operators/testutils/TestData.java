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

package org.apache.flink.runtime.operators.testutils;

import java.util.Comparator;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.MutableObjectIterator;

/**
 * Test data utilities classes.
 */
public final class TestData {
	
	/**
	 * Private constructor (container class should not be instantiated)
	 */
	private TestData() {}

	/**
	 * Key comparator.
	 */
	public static class KeyComparator implements Comparator<Key> {
		@Override
		public int compare(Key k1, Key k2) {
			return k1.compareTo(k2);
		}
	};

	/**
	 * Key implementation.
	 */
	public static class Key extends IntValue {
		private static final long serialVersionUID = 1L;
		
		public Key() {
			super();
		}

		public Key(int k) {
			super(k);
		}

		public int getKey() {
			return getValue();
		}
		
		public void setKey(int key) {
			setValue(key);
		}
	}

	/**
	 * Value implementation.
	 */
	public static class Value extends StringValue {
		
		private static final long serialVersionUID = 1L;

		public Value() {
			super();
		}

		public Value(String v) {
			super(v);
		}
		
		@Override
		public boolean equals(final Object obj) {
			if (this == obj) {
				return true;
			}
			
			if (obj.getClass() == TestData.Value.class) {
				final StringValue other = (StringValue) obj;
				int len = this.length();
				
				if (len == other.length()) {
					final char[] tc = this.getCharArray();
					final char[] oc = other.getCharArray();
					int i = 0, j = 0;
					
					while (len-- != 0) {
						if (tc[i++] != oc[j++]) {
							return false;
						}
					}
					return true;
				}
			}
			return false;
		}
	}

	/**
	 * Pair generator.
	 */
	public static class Generator implements MutableObjectIterator<Record> {
		
		public enum KeyMode {
			SORTED, RANDOM
		};

		public enum ValueMode {
			FIX_LENGTH, RANDOM_LENGTH, CONSTANT
		};

		private static char[] alpha = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'a', 'b', 'c',
			'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm' };

		private final long seed;

		private final int keyMax;

		private final int valueLength;

		private final KeyMode keyMode;

		private final ValueMode valueMode;

		private Random random;

		private int counter;

		private Key key;
		private Value value;

		public Generator(long seed, int keyMax, int valueLength) {
			this(seed, keyMax, valueLength, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
		}

		public Generator(long seed, int keyMax, int valueLength, KeyMode keyMode, ValueMode valueMode) {
			this(seed, keyMax, valueLength, keyMode, valueMode, null);
		}
		
		public Generator(long seed, int keyMax, int valueLength, KeyMode keyMode, ValueMode valueMode, Value constant) {
			this.seed = seed;
			this.keyMax = keyMax;
			this.valueLength = valueLength;
			this.keyMode = keyMode;
			this.valueMode = valueMode;

			this.random = new Random(seed);
			this.counter = 0;
			
			this.key = new Key();
			this.value = constant == null ? new Value() : constant;
		}

		public Record next(Record reuse) {
			this.key.setKey(keyMode == KeyMode.SORTED ? ++counter : Math.abs(random.nextInt() % keyMax) + 1);
			if (this.valueMode != ValueMode.CONSTANT) {
				this.value.setValue(randomString());
			}
			reuse.setField(0, this.key);
			reuse.setField(1, this.value);
			return reuse;
		}

		public Record next() {
			return next(new Record(2));
		}

		public boolean next(org.apache.flink.types.Value[] target) {
			this.key.setKey(keyMode == KeyMode.SORTED ? ++counter : Math.abs(random.nextInt() % keyMax) + 1);
			// TODO change this to something proper
			((IntValue)target[0]).setValue(this.key.getValue());
			((IntValue)target[1]).setValue(random.nextInt());
			return true;
		}

		public int sizeOf(Record rec) {
			// key
			int valueLength = Integer.SIZE / 8;

			// value
			String text = rec.getField(1, Value.class).getValue();
			int strlen = text.length();
			int utflen = 0;
			int c;
			for (int i = 0; i < strlen; i++) {
				c = text.charAt(i);
				if ((c >= 0x0001) && (c <= 0x007F)) {
					utflen++;
				} else if (c > 0x07FF) {
					utflen += 3;
				} else {
					utflen += 2;
				}
			}
			valueLength += 2 + utflen;

			return valueLength;
		}

		public void reset() {
			this.random = new Random(seed);
			this.counter = 0;
		}

		private String randomString() {
			int length;

			if (valueMode == ValueMode.FIX_LENGTH) {
				length = valueLength;
			} else {
				length = valueLength - random.nextInt(valueLength / 3);
			}

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < length; i++) {
				sb.append(alpha[random.nextInt(alpha.length)]);
			}
			return sb.toString();
		}

	}
	
	/**
	 * Record reader mock.
	 */
	public static class GeneratorIterator implements MutableObjectIterator<Record> {
		
		private final Generator generator;

		private final int numberOfRecords;

		private int counter;

		public GeneratorIterator(Generator generator, int numberOfRecords) {
			this.generator = generator;
			this.generator.reset();
			this.numberOfRecords = numberOfRecords;
			this.counter = 0;
		}

		@Override
		public Record next(Record target) {
			if (counter < numberOfRecords) {
				counter++;
				return generator.next(target);
			}
			else {
				return null;
			}
		}

		@Override
		public Record next() {
			if (counter < numberOfRecords) {
				counter++;
				return generator.next();
			}
			else {
				return null;
			}
		}
		
		public void reset() {
			this.counter = 0;
		}
	}

	/**
	 * Tuple2<Integer, String> generator.
	 */
	public static class TupleGenerator implements MutableObjectIterator<Tuple2<Integer, String>> {

		public enum KeyMode {
			SORTED, RANDOM, SORTED_SPARSE
		};

		public enum ValueMode {
			FIX_LENGTH, RANDOM_LENGTH, CONSTANT
		};

		private static char[] alpha = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'a', 'b', 'c',
				'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm' };

		private final long seed;

		private final int keyMax;

		private final float keyDensity;

		private final int valueLength;

		private final KeyMode keyMode;

		private final ValueMode valueMode;

		private Random random;

		private int counter;

		private int key;
		private String value;

		public TupleGenerator(long seed, int keyMax, int valueLength) {
			this(seed, keyMax, valueLength, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
		}

		public TupleGenerator(long seed, int keyMax, int valueLength, KeyMode keyMode, ValueMode valueMode) {
			this(seed, keyMax, valueLength, keyMode, valueMode, null);
		}

		public TupleGenerator(long seed, int keyMax, int valueLength, KeyMode keyMode, ValueMode valueMode, String constant) {
			this(seed, keyMax, 1.0f, valueLength, keyMode, valueMode, constant);
		}

		public TupleGenerator(long seed, int keyMax, float keyDensity, int valueLength, KeyMode keyMode, ValueMode valueMode, String constant) {
			this.seed = seed;
			this.keyMax = keyMax;
			this.keyDensity = keyDensity;
			this.valueLength = valueLength;
			this.keyMode = keyMode;
			this.valueMode = valueMode;

			this.random = new Random(seed);
			this.counter = 0;

			this.value = constant == null ? null : constant;
		}

		public Tuple2<Integer, String> next(Tuple2<Integer, String> reuse) {
			this.key = nextKey();
			if (this.valueMode != ValueMode.CONSTANT) {
				this.value = randomString();
			}
			reuse.setFields(this.key, this.value);
			return reuse;
		}

		public Tuple2<Integer, String> next() {
			return next(new Tuple2<Integer, String>());
		}

		public boolean next(org.apache.flink.types.Value[] target) {
			this.key = nextKey();
			// TODO change this to something proper
			((IntValue)target[0]).setValue(this.key);
			((IntValue)target[1]).setValue(random.nextInt());
			return true;
		}

		private int nextKey() {
			if (keyMode == KeyMode.SORTED) {
				return ++counter;
			} else if (keyMode == KeyMode.SORTED_SPARSE) {
				int max = (int) (1 / keyDensity);
				counter += random.nextInt(max) + 1;
				return counter;
			} else {
				return Math.abs(random.nextInt() % keyMax) + 1;
			}
		}

		public void reset() {
			this.random = new Random(seed);
			this.counter = 0;
		}

		private String randomString() {
			int length;

			if (valueMode == ValueMode.FIX_LENGTH) {
				length = valueLength;
			} else {
				length = valueLength - random.nextInt(valueLength / 3);
			}

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < length; i++) {
				sb.append(alpha[random.nextInt(alpha.length)]);
			}
			return sb.toString();
		}

	}


	/**
	 * Record reader mock.
	 */
	public static class TupleGeneratorIterator implements MutableObjectIterator<Tuple2<Integer, String>> {

		private final TupleGenerator generator;

		private final int numberOfRecords;

		private int counter;

		public TupleGeneratorIterator(TupleGenerator generator, int numberOfRecords) {
			this.generator = generator;
			this.generator.reset();
			this.numberOfRecords = numberOfRecords;
			this.counter = 0;
		}

		@Override
		public Tuple2<Integer, String> next(Tuple2<Integer, String> target) {
			if (counter < numberOfRecords) {
				counter++;
				return generator.next(target);
			}
			else {
				return null;
			}
		}

		@Override
		public Tuple2<Integer, String> next() {
			if (counter < numberOfRecords) {
				counter++;
				return generator.next();
			}
			else {
				return null;
			}
		}

		public void reset() {
			this.counter = 0;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class ConstantValueIterator implements MutableObjectIterator<Record> {
		
		private final Key key;
		private final Value value;
		
		private final String valueValue;
		
		
		private final int numPairs;
		
		private int pos;
		
		
		public ConstantValueIterator(int keyValue, String valueValue, int numPairs) {
			this.key = new Key(keyValue);
			this.value = new Value();
			this.valueValue = valueValue;
			this.numPairs = numPairs;
		}
		
		@Override
		public Record next(Record reuse) {
			if (pos < this.numPairs) {
				this.value.setValue(this.valueValue + ' ' + pos);
				reuse.setField(0, this.key);
				reuse.setField(1, this.value);
				pos++;
				return reuse;
			}
			else {
				return null;
			}
		}

		@Override
		public Record next() {
			return next(new Record(2));
		}

		public void reset() {
			this.pos = 0;
		}
	}

	public static class TupleConstantValueIterator implements MutableObjectIterator<Tuple2<Integer, String>> {

		private int key;
		private String value;

		private final String valueValue;


		private final int numPairs;

		private int pos;


		public TupleConstantValueIterator(int keyValue, String valueValue, int numPairs) {
			this.key = keyValue;
			this.valueValue = valueValue;
			this.numPairs = numPairs;
		}

		@Override
		public Tuple2<Integer, String> next(Tuple2<Integer, String> reuse) {
			if (pos < this.numPairs) {
				this.value = this.valueValue + ' ' + pos;
				reuse.setFields(this.key, this.value);
				pos++;
				return reuse;
			}
			else {
				return null;
			}
		}

		@Override
		public Tuple2<Integer, String> next() {
			return next(new Tuple2<Integer, String>());
		}

		public void reset() {
			this.pos = 0;
		}
	}
}
