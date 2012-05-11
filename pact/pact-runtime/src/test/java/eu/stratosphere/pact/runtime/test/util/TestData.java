/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.test.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

/**
 * Test data utilities classes.
 * 
 * @author Alexander Alexandrov
 * @author Erik Nijkamp
 */
public final class TestData {
	/**
	 * Private constructor (container class should not be instantiated)
	 */
	private TestData() {
	}

	/**
	 * Key comparator.
	 */
	public static class KeyComparator implements Comparator<Key> {
		@Override
		public int compare(Key k1, Key k2) {
			return k1.compareTo(k2);
		}
	}

	/**
	 * Key implementation.
	 */
	public static class Key extends PactInteger {
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
	public static class Value extends PactString {

		public Value() {
			super();
		}

		public Value(String v) {
			super(v);
		}
		
		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(final Object obj)
		{
			if (this == obj) {
				return true;
			}
			
			if (obj.getClass() == TestData.Value.class) {
				final PactString other = (PactString) obj;
				int len = this.length();
				
				if (len == other.length()) {
					final char[] tc = this.getCharArray();
					final char[] oc = other.getCharArray();
					int i = 0, j = 0;
					
					while (len-- != 0) {
						if (tc[i++] != oc[j++]) return false;
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
	public static class Generator implements MutableObjectIterator<PactRecord>{
		public enum KeyMode {
			SORTED, RANDOM
		}

		public enum ValueMode {
			FIX_LENGTH, RANDOM_LENGTH, CONSTANT
		}

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

		public Generator(long seed, int keyMax, int valueLength, KeyMode keyMode, ValueMode valueMode)
		{
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

		public boolean next(PactRecord target) {
			this.key.setKey(keyMode == KeyMode.SORTED ? ++counter : Math.abs(random.nextInt() % keyMax) + 1);
			if (this.valueMode != ValueMode.CONSTANT) {
				this.value.setValue(randomString());
			}
			target.setField(0, this.key);
			target.setField(1, this.value);
			return true;
		}

		public int sizeOf(PactRecord rec) {
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

			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < length; i++) {
				sb.append(alpha[random.nextInt(alpha.length)]);
			}
			return sb.toString();
		}

	}

	/**
	 * Record reader mock.
	 */
	public static class RecordReaderIterMock implements eu.stratosphere.nephele.io.Reader<PactRecord> {
		private final Iterator<PactRecord> iterator;

		public RecordReaderIterMock(Iterator<PactRecord> iterator) {
			this.iterator = iterator;
		}

		public boolean hasNext() {
			return iterator.hasNext();
		}

		public PactRecord next() {
			return iterator.next();
		}
	}
	
	/**
	 * Record reader mock.
	 */
	public static class GeneratorIterator implements MutableObjectIterator<PactRecord>
	{
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
		public boolean next(PactRecord target) {
			if (counter < numberOfRecords) {
				counter++;
				return generator.next(target);
			}
			else {
				return false;
			}
		}
		
		public void reset() {
			this.counter = 0;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class ConstantValueIterator implements MutableObjectIterator<PactRecord>
	{
		private final Key key;
		private final Value value;
		
		private final String valueValue;
		
		
		private final int numPairs;
		
		private int pos;
		
		
		public ConstantValueIterator(int keyValue, String valueValue, int numPairs)
		{
			this.key = new Key(keyValue);
			this.value = new Value();			
			this.valueValue = valueValue;
			this.numPairs = numPairs;
		}
		
		@Override
		public boolean next(PactRecord target) {
			if (pos < this.numPairs) {
				this.value.setValue(this.valueValue + ' ' + pos);
				target.setField(0, this.key);
				target.setField(1, this.value);
				pos++;
				return true;
			}
			else {
				return false;
			}
		}
		
		public void reset() {
			this.pos = 0;
		}
	}
}
