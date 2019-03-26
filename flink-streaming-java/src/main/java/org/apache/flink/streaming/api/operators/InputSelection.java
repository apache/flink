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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Describe the input selection that operators want to get the next record.
 */
@PublicEvolving
public final class InputSelection {

	/**
	 * The {@code InputSelection} instance which indicates to select all inputs.
	 */
	public static final InputSelection ALL = new InputSelectionBuilder().select(-1).build();

	/**
	 * The {@code InputSelection} instance which indicates to select the first input.
	 */
	public static final InputSelection FIRST = new InputSelectionBuilder().select(1).build();

	/**
	 * The {@code InputSelection} instance which indicates to select the second input.
	 */
	public static final InputSelection SECOND = new InputSelectionBuilder().select(2).build();

	private final long inputMask;

	private InputSelection(long inputMask) {
		this.inputMask = inputMask;
	}

	public long getInputMask() {
		return inputMask;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		InputSelection that = (InputSelection) o;
		return inputMask == that.inputMask;
	}

	@Override
	public String toString() {
		return String.valueOf(inputMask);
	}

	/**
	 * Utility class for creating {@link InputSelection}.
	 */
	public static final class InputSelectionBuilder {

		private long inputMask = 0;

		/**
		 * Selects an input identified by the given {@code inputId}.
		 *
		 * @param inputId
		 *     the input id numbered starting from 1 to 64, and `1` indicates the first input.
		 *     Specially, `-1` indicates all inputs.
		 * @return
		 */
		public InputSelectionBuilder select(long inputId) {
			if (inputId > 0 && inputId <= 64){
				inputMask |= 1L << (inputId - 1);
			} else if (inputId == -1L) {
				inputMask = -1L;
			} else {
				throw new IllegalArgumentException("The inputId must be in the range of 1 to 64, or be -1.");
			}

			return this;
		}

		public InputSelection build() {
			return new InputSelection(inputMask);
		}
	}
}
