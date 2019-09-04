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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/**
 * Defines the count of accepted arguments (including open intervals) that a function can take.
 */
@PublicEvolving
public interface ArgumentCount {

	/**
	 * Enables custom validation of argument counts after {@link #getMinCount()} and
	 * {@link #getMaxCount()} have been validated.
	 *
	 * @param count total number of arguments including each argument for a vararg function call
	 */
	boolean isValidCount(int count);

	/**
	 * Returns the minimum number of argument (inclusive) that a function can take.
	 *
	 * <p>{@link Optional#empty()} if such a lower bound is not defined.
	 */
	Optional<Integer> getMinCount();

	/**
	 * Returns the maximum number of argument (inclusive) that a function can take.
	 *
	 * <p>{@link Optional#empty()} if such an upper bound is not defined.
	 */
	Optional<Integer> getMaxCount();

	/**
	 * The argument count should to be exactly the same as count.
	 */
	static ArgumentCount exact(int count) {
		return new ArgumentCount() {

			@Override
			public boolean isValidCount(int validCount) {
				return count == validCount;
			}

			@Override
			public Optional<Integer> getMinCount() {
				return Optional.of(count);
			}

			@Override
			public Optional<Integer> getMaxCount() {
				return Optional.of(count);
			}
		};
	}

	/**
	 * The argument count should to be in the range of min and max.
	 */
	static ArgumentCount range(int min, int max) {
		return new ArgumentCount() {
			@Override
			public boolean isValidCount(int count) {
				return min <= count && count <= max;
			}

			@Override
			public Optional<Integer> getMinCount() {
				return Optional.of(min);
			}

			@Override
			public Optional<Integer> getMaxCount() {
				return Optional.of(max);
			}
		};
	}

	/**
	 * The argument count should bigger than min.
	 */
	static ArgumentCount atLeast(int min) {
		return new ArgumentCount() {
			@Override
			public boolean isValidCount(int count) {
				return min <= count;
			}

			@Override
			public Optional<Integer> getMinCount() {
				return Optional.of(min);
			}

			@Override
			public Optional<Integer> getMaxCount() {
				return Optional.empty();
			}
		};
	}

	/**
	 * Always pass on {@link ArgumentCount}.
	 */
	static ArgumentCount passing() {
		return new ArgumentCount() {

			@Override
			public boolean isValidCount(int count) {
				return true;
			}

			@Override
			public Optional<Integer> getMinCount() {
				return Optional.empty();
			}

			@Override
			public Optional<Integer> getMaxCount() {
				return Optional.empty();
			}
		};
	}
}
