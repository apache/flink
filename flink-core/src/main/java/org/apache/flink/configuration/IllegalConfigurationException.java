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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

/**
 * An {@code IllegalConfigurationException} is thrown when
 * the values in a given {@link Configuration} are not valid. This may refer
 * to the Flink configuration with which the framework is started,
 * or a Configuration passed internally between components.
 */
@PublicEvolving
public class IllegalConfigurationException extends RuntimeException {

	private static final long serialVersionUID = 695506964810499989L;

	/**
	 * Constructs an new IllegalConfigurationException with the given error message.
	 *
	 * @param message The error message for the exception.
	 */
	public IllegalConfigurationException(String message) {
		super(message);
	}

	/**
	 * Constructs an new IllegalConfigurationException with the given error message
	 * format and arguments.
	 *
	 * @param format The error message format for the exception.
	 * @param arguments The arguments for the format.
	 */
	public IllegalConfigurationException(String format, Object... arguments) {
		super(String.format(format, arguments));
	}

	/**
	 * Constructs an new IllegalConfigurationException with the given error message
	 * and a given cause.
	 *
	 * @param message The error message for the exception.
	 * @param cause The exception that caused this exception.
	 */
	public IllegalConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}
}
