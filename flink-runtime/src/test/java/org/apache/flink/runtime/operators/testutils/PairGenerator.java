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

import java.io.IOException;
import java.util.Random;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

public final class PairGenerator {

	public static class Pair implements Value {
		
		private static final long serialVersionUID = 1L;
		
		private int key;
		private StringValue value = new StringValue();
		
		
		public Pair() {}
		
		
		public int getKey() {
			return key;
		}
		
		public StringValue getValue() {
			return value;
		}
		
		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeInt(key);
			value.write(out);
		}
		
		@Override
		public void read(DataInputView in) throws IOException {
			key = in.readInt();
			value.read(in);
		}
		
		@Override
		public int hashCode() {
			return 31 * key + value.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Pair) {
				Pair other = (Pair) obj;
				return other.key == this.key && other.value.equals(this.value);
			} else {
				return false;
			}
		}
		
		@Override
		public String toString() {
			return String.format("(%d, %s)", key, value);
		}
	}
	
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

	private final StringValue valueConstant;

	
	public PairGenerator(long seed, int keyMax, int valueLength) {
		this(seed, keyMax, valueLength, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
	}

	public PairGenerator(long seed, int keyMax, int valueLength, KeyMode keyMode, ValueMode valueMode) {
		this(seed, keyMax, valueLength, keyMode, valueMode, null);
	}
	
	public PairGenerator(long seed, int keyMax, int valueLength, KeyMode keyMode, ValueMode valueMode, String constant) {
		this.seed = seed;
		this.keyMax = keyMax;
		this.valueLength = valueLength;
		this.keyMode = keyMode;
		this.valueMode = valueMode;

		this.random = new Random(seed);
		this.counter = 0;
		
		this.valueConstant = new StringValue();
		if (constant != null) {
			this.valueConstant.setValue(constant);
		}
	}

	public void next(Pair target) {
		target.key = (keyMode == KeyMode.SORTED ? ++counter : Math.abs(random.nextInt() % keyMax) + 1);
		
		if (valueMode == ValueMode.CONSTANT) {
			target.value = valueConstant;
		} else {
			randomString(target.value);
		}
	}

	public void reset() {
		this.random = new Random(seed);
		this.counter = 0;
	}

	private void randomString(StringValue target) {
		
		int length = valueMode == ValueMode.FIX_LENGTH ?
			valueLength :
			valueLength - random.nextInt(valueLength / 3);

		target.setLength(0);
		for (int i = 0; i < length; i++) {
			target.append(alpha[random.nextInt(alpha.length)]);
		}
	}
}
