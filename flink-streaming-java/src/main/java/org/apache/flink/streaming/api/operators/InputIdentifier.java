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
 * The identifier for the input(s) of the operator. It is numbered starting from 1, and
 * 1 indicates the first input. With one exception, -1 indicates all inputs.
 */
@PublicEvolving
public final class InputIdentifier {

	/**
	 * The {@code InputIdentifier} object corresponding to the input id {@code -1}.
	 */
	public static final InputIdentifier ALL = new InputIdentifier(-1);

	/**
	 * The {@code InputIdentifier} object corresponding to the input id {@code 1}.
	 */
	public static final InputIdentifier FIRST = new InputIdentifier(1);

	/**
	 * The {@code InputIdentifier} object corresponding to the input id {@code 2}.
	 */
	public static final InputIdentifier SECOND = new InputIdentifier(2);

	private final int inputId;

	private InputIdentifier(int inputId) {
		this.inputId = inputId;
	}

	public int getInputId() {
		return inputId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		InputIdentifier that = (InputIdentifier) o;
		return inputId == that.inputId;
	}

	@Override
	public String toString() {
		return String.valueOf(inputId);
	}
}
