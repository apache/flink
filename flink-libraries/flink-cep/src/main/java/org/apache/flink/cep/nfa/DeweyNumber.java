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

import java.io.Serializable;
import java.util.Arrays;

/**
 * Versioning scheme which allows to retrieve dependencies between different versions.
 *
 * A dewey number consists of a sequence of digits d1.d2.d3. ... .dn. A dewey number v is compatible
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

	protected DeweyNumber(int[] deweyNumber) {
		this.deweyNumber = deweyNumber;
	}

	/**
	 * Checks whether this dewey number is compatible to the other dewey number.
	 *
	 * True iff this contains other as a prefix or iff they differ only in the last digit whereas
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
		int[] newDeweyNumber = Arrays.copyOf(deweyNumber, deweyNumber.length);
		newDeweyNumber[deweyNumber.length - 1]++;

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
}
