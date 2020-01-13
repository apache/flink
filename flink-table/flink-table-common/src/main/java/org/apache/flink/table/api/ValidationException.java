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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Exception for all errors occurring during validation phase.
 *
 * <p>This exception indicates that the user did something wrong.
 *
 * @see TableException
 */
@PublicEvolving
public final class ValidationException extends RuntimeException {

	/**
	 * Unformatted exception with cause.
	 *
	 * @see ValidationException#ValidationException(Throwable, String, Object...)
	 */
	public ValidationException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Unformatted exception.
	 *
	 * @see ValidationException#ValidationException(String, Object...)
	 */
	public ValidationException(String message) {
		super(message);
	}

	/**
	 * Creates a formatted exception for a unified exception experience.
	 *
	 * <p>Please use the following convention:
	 *
	 * <p><ul>
	 * <li>Start with a capital letter.
	 * <li>Surround strings with single quotes. Numbers must not be surrounded.
	 * <li>End message sentences with a period.
	 * </ul>
	 *
	 * <p>E.g.: {@code new ValidationException("Function class '%s' at position %d is invalid.", clazz.getName(), pos)}
	 */
	public ValidationException(String format, Object ... args) {
		this(String.format(format, args));
	}

	/**
	 * Creates a formatted exception with cause for a unified exception experience.
	 *
	 * <p>Please use the following convention:
	 *
	 * <p><ul>
	 * <li>Start with a capital letter.
	 * <li>Surround strings with single quotes. Numbers must not be surrounded.
	 * <li>End message sentences with a period.
	 * </ul>
	 *
	 * <p>E.g.: {@code new ValidationException(t, "Function class '%s' at position %d is invalid.", clazz.getName(), pos)}
	 */
	public ValidationException(Throwable cause, String format, Object... args) {
		this(String.format(format, args), cause);
	}
}
