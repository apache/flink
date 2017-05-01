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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Enumeration representing the result of reconfiguring a {@link TypeSerializer}
 * with a configuration snapshot of a preceding serializer.
 */
@PublicEvolving
public enum ReconfigureResult {

	/**
	 * Represents that the serializer is reconfigured to be compatible with
	 * its predecessor, and the serialization schema remains the same.
	 */
	COMPATIBLE(0),

	/**
	 * Represents that the serializer is reconfigured to be compatible with
	 * its predecessor, but has a new serialization schema.
	 */
	COMPATIBLE_NEW_SCHEMA(1),

	/**
	 * Represents that the serializer cannot be reconfigured to be compatible with its predecessor.
	 */
	INCOMPATIBLE(2);

	private int precedence;

	ReconfigureResult(int precedence) {
		this.precedence = precedence;
	}

	/**
	 * Takes two reconfiguration results, and returns the result with higher precedence.
	 *
	 * @param first the first reconfiguration result.
	 * @param second the second reconfiguration result.
	 *
	 * @return the result with the higher precedence.
	 */
	public static ReconfigureResult resolvePrecedence(ReconfigureResult first, ReconfigureResult second) {
		return (first.precedence > second.precedence) ? first : second;
	}
}
