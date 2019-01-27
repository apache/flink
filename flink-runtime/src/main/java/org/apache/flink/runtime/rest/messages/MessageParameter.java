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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.util.Preconditions;

/**
 * This class represents a single path/query parameter that can be used for a request. Every parameter has an associated
 * key, and a one-time settable value.
 *
 * <p>Parameters are either mandatory or optional, indicating whether the parameter must be resolved for the request.
 *
 * <p>All parameters support symmetric conversion from their actual type and string via {@link #convertFromString(String)}
 * and {@link #convertToString(Object)}. The conversion from {@code X} to string is required on the client to assemble the
 * URL, whereas the conversion from string to {@code X} is required on the server to provide properly typed parameters
 * to the handlers.
 *
 * @see MessagePathParameter
 * @see MessageQueryParameter
 */
public abstract class MessageParameter<X> {
	private boolean resolved = false;

	private final MessageParameterRequisiteness requisiteness;

	private final String key;
	private X value;

	protected MessageParameter(String key, MessageParameterRequisiteness requisiteness) {
		this.key = Preconditions.checkNotNull(key);
		this.requisiteness = Preconditions.checkNotNull(requisiteness);
	}

	/**
	 * Returns whether this parameter has been resolved.
	 *
	 * @return true, if this parameter was resolved, false otherwise
	 */
	public final boolean isResolved() {
		return resolved;
	}

	/**
	 * Resolves this parameter for the given value.
	 *
	 * @param value value to resolve this parameter with
	 */
	public final void resolve(X value) {
		Preconditions.checkState(!resolved, "This parameter was already resolved.");
		this.value = Preconditions.checkNotNull(value);
		this.resolved = true;
	}

	/**
	 * Resolves this parameter for the given string value representation.
	 *
	 * @param value string representation of value to resolve this parameter with
	 */
	public final void resolveFromString(String value) throws ConversionException {
		resolve(convertFromString(value));
	}

	/**
	 * Converts the given string to a valid value of this parameter.
	 *
	 * @param value string representation of parameter value
	 * @return parameter value
	 */
	protected abstract X convertFromString(String value) throws ConversionException;

	/**
	 * Converts the given value to its string representation.
	 *
	 * @param value parameter value
	 * @return string representation of typed value
	 */
	protected abstract String convertToString(X value);

	/**
	 * Returns the key of this parameter, e.g. "jobid".
	 *
	 * @return key of this parameter
	 */
	public final String getKey() {
		return key;
	}

	/**
	 * Returns the resolved value of this parameter, or {@code null} if it isn't resolved yet.
	 *
	 * @return resolved value, or null if it wasn't resolved yet
	 */
	public final X getValue() {
		return value;
	}

	/**
	 * Returns the resolved value of this parameter as a string, or {@code null} if it isn't resolved yet.
	 *
	 * @return resolved value, or null if it wasn't resolved yet
	 */
	final String getValueAsString() {
		return value == null
			? null
			: convertToString(value);
	}

	/**
	 * Returns whether this parameter must be resolved for the request.
	 *
	 * @return true if the parameter is mandatory, false otherwise
	 */
	public final boolean isMandatory() {
		return requisiteness == MessageParameterRequisiteness.MANDATORY;
	}

	/**
	 * Enum for indicating whether a parameter is mandatory or optional.
	 */
	protected enum MessageParameterRequisiteness {
		MANDATORY,
		OPTIONAL
	}
}
