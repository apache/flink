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
 * <p>Subclasses are encouraged to provide their own {@code resolve()} method that accepts the parameter value in a more
 * natural fashion (like accepting a JobID), and convert it to a string before calling {@link #resolve(String)}.
 *
 * @see MessagePathParameter
 * @see MessageQueryParameter
 */
public abstract class MessageParameter {
	private boolean resolved = false;

	private final MessageParameterType type;
	private final MessageParameterRequisiteness requisiteness;

	private final String key;
	private String value;

	MessageParameter(String key, MessageParameterType type, MessageParameterRequisiteness requisiteness) {
		this.key = key;
		this.type = type;
		this.requisiteness = requisiteness;
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
	public final void resolve(String value) {
		Preconditions.checkState(!resolved, "This parameter was already resolved.");
		this.value = value;
		this.resolved = true;
	}

	/**
	 * Returns the key of this parameter, e.g. "jobid".
	 *
	 * @return key of this parameter
	 */
	public final String getKey() {
		return key;
	}

	/**
	 * Returs the resolved value of this parameter, or {@code null} if it isn't resolved yet.
	 *
	 * @return resolved value, or null if it wasn't resolved yet
	 */
	public final String getValue() {
		return value;
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
	 * Returns the type of this parameter, i.e. whether it is a path or query parameter.
	 *
	 * @return parameter type
	 */
	public final MessageParameterType getParameterType() {
		return type;
	}

	/**
	 * Enum for indicating whether a parameter is a path or query parameter.
	 */
	public enum MessageParameterType {
		QUERY,
		PATH
	}

	/**
	 * Enum for indicating whether a parameter is mandatory or optional.
	 */
	protected enum MessageParameterRequisiteness {
		MANDATORY,
		OPTIONAL
	}
}
