/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.types;

/**
 * A boolean value with a third, "unset" state.
 */
public class OptionalBoolean {

	private final boolean defaultValue;

	private Boolean value = null;

	/**
	 * An {@code OptionalBoolean} has three possible states: true, false, and
	 * "unset". The value is set when merged with a value of true or false. The
	 * state returns to unset either explicitly or when true is merged with false.
	 *
	 * @param defaultValue the value to return when the object's state is unset
	 */
	public OptionalBoolean(boolean defaultValue) {
		this.defaultValue = defaultValue;
	}

	/**
	 * Get the boolean state.
	 *
	 * @return boolean state
	 */
	public boolean get() {
		return (value == null) ? defaultValue : value;
	}

	/**
	 * Set the boolean state.
	 *
	 * @param value boolean state
	 */
	public void set(boolean value) {
		this.value = value;
	}

	/**
	 * Reset to the unset state.
	 */
	public void unset() {
		this.value = null;
	}

	/**
	 * The mismatched states are true with false and false with true.
	 *
	 * @param other object to test for mismatch
	 * @return whether the objects are mismatched
	 */
	public boolean isMismatchedWith(OptionalBoolean other) {
		return (value != null && other.value != null && !value.equals(other.value));
	}

	/**
	 * State transitions:
	 *  if the states are the same then keep the same state
	 *  if the states are mismatched then change to the unset state
	 *  if in an unset state then change to the other state
	 *
	 * @param other object from which to merge state
	 */
	public void mergeWith(OptionalBoolean other) {
		if ((value == null && other.value == null) || isMismatchedWith(other)) {
			value = null;
		} else {
			value = (value == null) ? other.value : value;
		}
	}
}
