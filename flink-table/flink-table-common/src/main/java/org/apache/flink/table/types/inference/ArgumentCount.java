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
 *
 * <p>Note: Implementations should implement {@link Object#hashCode()} and {@link Object#equals(Object)}.
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
}
