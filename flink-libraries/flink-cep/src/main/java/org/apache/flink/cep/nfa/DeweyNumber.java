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

public class DeweyNumber implements Serializable {

	private static final long serialVersionUID = 6170434818252267825L;
	private final int[] deweyNumber;

	public DeweyNumber(int start) {
		deweyNumber = new int[]{start};
	}

	protected DeweyNumber(int[] deweyNumber) {
		this.deweyNumber = deweyNumber;
	}

	public boolean isCompatibleWith(DeweyNumber other) {
		if (length() > other.length()) {
			for (int i = 0; i < other.length(); i++) {
				if (other.deweyNumber[i] != deweyNumber[i]) {
					return false;
				}
			}

			return true;
		} else if (length() == other.length()) {
			int lastIndex = length() - 1;
			for (int i = 0; i < lastIndex; i++) {
				if (other.deweyNumber[i] != deweyNumber[i]) {
					return false;
				}
			}

			return deweyNumber[lastIndex] >= other.deweyNumber[lastIndex];
		} else {
			return false;
		}
	}

	public int length() {
		return deweyNumber.length;
	}

	public DeweyNumber increase() {
		int[] newDeweyNumber = Arrays.copyOf(deweyNumber, deweyNumber.length);
		newDeweyNumber[deweyNumber.length - 1]++;

		return new DeweyNumber(newDeweyNumber);
	}

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
