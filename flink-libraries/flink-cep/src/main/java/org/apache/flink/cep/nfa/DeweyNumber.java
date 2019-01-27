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

package org.apache.flink.cep.nfa;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Versioning scheme which allows to retrieve dependencies between different versions.
 *
 * <p>A dewey number consists of a sequence of digits d1.d2.d3. ... .dn. A dewey number v is compatible
 * to v' iff v contains v' as a prefix or if both dewey number differ only in the last digit and
 * the last digit of v is greater than v'.
 *
 */
public class DeweyNumber implements Serializable {

	private static final long serialVersionUID = 6170434818252267825L;

	// sequence of digits
	private final int[] deweyNumber;

	public DeweyNumber(int start) {
		deweyNumber = new int[]{start};
	}

	public DeweyNumber(DeweyNumber number) {
		this.deweyNumber = Arrays.copyOf(number.deweyNumber, number.deweyNumber.length);
	}

	private DeweyNumber(int[] deweyNumber) {
		this.deweyNumber = deweyNumber;
	}

	/**
	 * Checks whether this dewey number is compatible to the other dewey number.
	 *
	 * <p>True iff this contains other as a prefix or iff they differ only in the last digit whereas
	 * the last digit of this is greater than the last digit of other.
	 *
	 * @param other The other dewey number to check compatibility against
	 * @return Whether this dewey number is compatible to the other dewey number
	 */
	public boolean isCompatibleWith(DeweyNumber other) {
		if (length() > other.length()) {
			// prefix case
			for (int i = 0; i < other.length(); i++) {
				if (other.deweyNumber[i] != deweyNumber[i]) {
					return false;
				}
			}

			return true;
		} else if (length() == other.length()) {
			// check init digits for equality
			int lastIndex = length() - 1;
			for (int i = 0; i < lastIndex; i++) {
				if (other.deweyNumber[i] != deweyNumber[i]) {
					return false;
				}
			}

			// check that the last digit is greater or equal
			return deweyNumber[lastIndex] >= other.deweyNumber[lastIndex];
		} else {
			return false;
		}
	}

	public int getRun() {
		return deweyNumber[0];
	}

	public int length() {
		return deweyNumber.length;
	}

	/**
	 * Creates a new dewey number from this such that its last digit is increased by
	 * one.
	 *
	 * @return A new dewey number derived from this whose last digit is increased by one
	 */
	public DeweyNumber increase() {
		return increase(1);
	}

	/**
	 * Creates a new dewey number from this such that its last digit is increased by the supplied
	 * number.
	 *
	 * @param times how many times to increase the Dewey number
	 * @return A new dewey number derived from this whose last digit is increased by given number
	 */
	public DeweyNumber increase(int times) {
		int[] newDeweyNumber = Arrays.copyOf(deweyNumber, deweyNumber.length);
		newDeweyNumber[deweyNumber.length - 1] += times;

		return new DeweyNumber(newDeweyNumber);
	}

	/**
	 * Creates a new dewey number from this such that a 0 is appended as new last digit.
	 *
	 * @return A new dewey number which contains this as a prefix and has 0 as last digit
	 */
	public DeweyNumber addStage() {
		int[] newDeweyNumber = Arrays.copyOf(deweyNumber, deweyNumber.length + 1);

		return new DeweyNumber(newDeweyNumber);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DeweyNumber) {
			DeweyNumber other = (DeweyNumber) obj;

			return Arrays.equals(deweyNumber, other.deweyNumber);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(deweyNumber);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < length() - 1; i++) {
			builder.append(deweyNumber[i]).append(".");
		}

		if (length() > 0) {
			builder.append(deweyNumber[length() - 1]);
		}

		return builder.toString();
	}

	/**
	 * Creates a dewey number from a string representation. The input string must be a dot separated
	 * string of integers.
	 *
	 * @param deweyNumberString Dot separated string of integers
	 * @return Dewey number generated from the given input string
	 */
	public static DeweyNumber fromString(final String deweyNumberString) {
		String[] splits = deweyNumberString.split("\\.");

		if (splits.length == 0) {
			return new DeweyNumber(Integer.parseInt(deweyNumberString));
		} else {
			int[] deweyNumber = new int[splits.length];

			for (int i = 0; i < splits.length; i++) {
				deweyNumber[i] = Integer.parseInt(splits[i]);
			}

			return new DeweyNumber(deweyNumber);
		}
	}

	/**
	 * A {@link TypeSerializer} for the {@link DeweyNumber} which serves as a version number.
	 */
	public static class DeweyNumberSerializer extends TypeSerializerSingleton<DeweyNumber> {

		private static final long serialVersionUID = -5086792497034943656L;

		private final IntSerializer elemSerializer = IntSerializer.INSTANCE;

		public static final DeweyNumberSerializer INSTANCE = new DeweyNumberSerializer();

		private DeweyNumberSerializer() {}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public DeweyNumber createInstance() {
			return new DeweyNumber(1);
		}

		@Override
		public DeweyNumber copy(DeweyNumber from) {
			return new DeweyNumber(from);
		}

		@Override
		public DeweyNumber copy(DeweyNumber from, DeweyNumber reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(DeweyNumber record, DataOutputView target) throws IOException {
			final int size = record.length();
			target.writeInt(size);
			for (int i = 0; i < size; i++) {
				elemSerializer.serialize(record.deweyNumber[i], target);
			}
		}

		@Override
		public DeweyNumber deserialize(DataInputView source) throws IOException {
			final int size = source.readInt();
			int[] number = new int[size];
			for (int i = 0; i < size; i++) {
				number[i] = elemSerializer.deserialize(source);
			}
			return new DeweyNumber(number);
		}

		@Override
		public DeweyNumber deserialize(DeweyNumber reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			final int size = source.readInt();
			target.writeInt(size);
			for (int i = 0; i < size; i++) {
				elemSerializer.copy(source, target);
			}
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this || obj.getClass().equals(getClass());
		}

		@Override
		public boolean canEqual(Object obj) {
			return true;
		}

		@Override
		public int hashCode() {
			return elemSerializer.hashCode();
		}
	}
}
