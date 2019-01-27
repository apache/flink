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

package org.apache.flink.table.api.functions;

/**
 * Enumeration of types of monotonicity.
 */
public enum Monotonicity {
	/**
	 * An user defined function {@code f} is increasing within a stream if the value
	 * of {@code f} for a given row is always greater than or equal to the value in
	 * the previous row.
	 */
	INCREASING,

	/**
	 * An user defined function {@code f} is decreasing within a stream if the value
	 * of {@code f} for a given row is always less than or equal to the value in
	 * the previous row.
	 */
	DECREASING,

	/**
	 * Neither INCREASING nor DECREASING.
	 */
	NOT_MONOTONIC;

	/**
	 * Whether values of this monotonicity are increasing. That is, if a value
	 * at a given point in a sequence is X, no point later in the sequence will
	 * have a value less than X.
	 *
	 * @return whether values are decreasing
	 */
	public boolean isIncreasing() {
		switch (this) {
			case INCREASING:
				return true;
			default:
				return false;
		}
	}

	/**
	 * Whether values of this monotonicity are decreasing. That is, if a value
	 * at a given point in a sequence is X, no point later in the sequence will
	 * have a value greater than X.
	 *
	 * @return whether values are decreasing
	 */
	public boolean isDecreasing() {
		switch (this) {
			case DECREASING:
				return true;
			default:
				return false;
		}
	}
}
