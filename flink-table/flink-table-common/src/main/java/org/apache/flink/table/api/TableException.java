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
 * General Exception for all errors during table handling.
 *
 * <p>This exception indicates that an internal error occurred or that a feature is not supported
 * yet. Usually, this exception does not indicate a fault of the user.
 *
 * @see ValidationException
 */
@PublicEvolving
public final class TableException extends RuntimeException {

	/**
	 * Unformatted exception with cause.
	 *
	 * @see TableException#TableException(Throwable, String, Object...)
	 */
	public TableException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Unformatted exception.
	 *
	 * @see TableException#TableException(String, Object...)
	 */
	public TableException(String message) {
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
	 * <p>E.g.: {@code new TableException("Function class '%s' is not supported yet.", clazz.getName())}
	 */
	public TableException(String format, Object... args) {
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
	 * <p>E.g.: {@code new TableException(t, "Function class '%s' is not supported yet.", clazz.getName())}
	 */
	public TableException(Throwable cause, String format, Object... args) {
		this(String.format(format, args), cause);
	}
}
