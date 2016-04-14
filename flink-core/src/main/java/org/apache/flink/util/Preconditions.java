/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ----------------------------------------------------------------------------
//  This class is largely adapted from "com.google.common.base.Preconditions",
//  which is part of the "Guava" library.
//
//  Because of frequent issues with dependency conflicts, this class was
//  added to the Flink code base to reduce dependency on Guava.
// ----------------------------------------------------------------------------

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

/**
 * A collection of static utility methods to validate input.
 * 
 * <p>This class is modelled after Google Guava's Preconditions class, and partly takes code
 * from that class. We add this code to the Flink code base in order to reduce external
 * dependencies.
 */
@Internal
public final class Preconditions {

	// ------------------------------------------------------------------------
	//  Null checks
	// ------------------------------------------------------------------------
	
	/**
	 * Ensures that the given object reference is not null.
	 * Upon violation, a {@code NullPointerException} with no message is thrown.
	 * 
	 * @param reference The object reference
	 * @return The object reference itself (generically typed).
	 * 
	 * @throws NullPointerException Thrown, if the passed reference was null.
	 */
	public static <T> T checkNotNull(T reference) {
		if (reference == null) {
			throw new NullPointerException();
		}
		return reference;
	}
	
	/**
	 * Ensures that the given object reference is not null.
	 * Upon violation, a {@code NullPointerException} with the given message is thrown.
	 * 
	 * @param reference The object reference
	 * @param errorMessage The message for the {@code NullPointerException} that is thrown if the check fails.
	 * @return The object reference itself (generically typed).
	 *
	 * @throws NullPointerException Thrown, if the passed reference was null.
	 */
	public static <T> T checkNotNull(T reference, @Nullable String errorMessage) {
		if (reference == null) {
			throw new NullPointerException(String.valueOf(errorMessage));
		}
		return reference;
	}

	/**
	 * Ensures that the given object reference is not null.
	 * Upon violation, a {@code NullPointerException} with the given message is thrown.
	 * 
	 * <p>The error message is constructed from a template and an arguments array, after
	 * a similar fashion as {@link String#format(String, Object...)}, but supporting only
	 * {@code %s} as a placeholder.
	 *
	 * @param reference The object reference
	 * @param errorMessageTemplate The message template for the {@code NullPointerException}
	 *                             that is thrown if the check fails. The template substitutes its
	 *                             {@code %s} placeholders with the error message arguments.
	 * @param errorMessageArgs The arguments for the error message, to be inserted into the
	 *                         message template for the {@code %s} placeholders.
	 *                         
	 * @return The object reference itself (generically typed).
	 *
	 * @throws NullPointerException Thrown, if the passed reference was null.
	 */
	public static <T> T checkNotNull(T reference,
				@Nullable String errorMessageTemplate,
				@Nullable Object... errorMessageArgs) {
		
		if (reference == null) {
			throw new NullPointerException(format(errorMessageTemplate, errorMessageArgs));
		}
		return reference;
	}

	// ------------------------------------------------------------------------
	//  Boolean Condition Checking
	// ------------------------------------------------------------------------
	
	/**
	 * Checks the given boolean condition, and throws an {@code IllegalArgumentException} if
	 * the condition is not met (evaluates to {@code false}).
	 *
	 * @param condition The condition to check
	 *                     
	 * @throws IllegalArgumentException Thrown, if the condition is violated.
	 */
	public static void checkArgument(boolean condition) {
		if (!condition) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Checks the given boolean condition, and throws an {@code IllegalArgumentException} if
	 * the condition is not met (evaluates to {@code false}). The exception will have the
	 * given error message.
	 *
	 * @param condition The condition to check
	 * @param errorMessage The message for the {@code IllegalArgumentException} that is thrown if the check fails.
	 * 
	 * @throws IllegalArgumentException Thrown, if the condition is violated.
	 */
	public static void checkArgument(boolean condition, @Nullable Object errorMessage) {
		if (!condition) {
			throw new IllegalArgumentException(String.valueOf(errorMessage));
		}
	}

	/**
	 * Checks the given boolean condition, and throws an {@code IllegalArgumentException} if
	 * the condition is not met (evaluates to {@code false}).
	 *
	 * @param condition The condition to check
	 * @param errorMessageTemplate The message template for the {@code IllegalArgumentException}
	 *                             that is thrown if the check fails. The template substitutes its
	 *                             {@code %s} placeholders with the error message arguments.
	 * @param errorMessageArgs The arguments for the error message, to be inserted into the
	 *                         message template for the {@code %s} placeholders.
	 * 
	 * @throws IllegalArgumentException Thrown, if the condition is violated.
	 */
	public static void checkArgument(boolean condition,
			@Nullable String errorMessageTemplate,
			@Nullable Object... errorMessageArgs) {
		
		if (!condition) {
			throw new IllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));
		}
	}


	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * A simplified formatting method. Similar to {@link String#format(String, Object...)}, but
	 * with lower overhead (only String parameters, no locale, no format validation).
	 * 
	 * <p>This method is taken quasi verbatim from the Guava Preconditions class.
	 */
	private static String format(@Nullable String template, @Nullable Object... args) {
		final int numArgs = args == null ? 0 : args.length;
		template = String.valueOf(template); // null -> "null"
		
		// start substituting the arguments into the '%s' placeholders
		StringBuilder builder = new StringBuilder(template.length() + 16 * numArgs);
		int templateStart = 0;
		int i = 0;
		while (i < numArgs) {
			int placeholderStart = template.indexOf("%s", templateStart);
			if (placeholderStart == -1) {
				break;
			}
			builder.append(template.substring(templateStart, placeholderStart));
			builder.append(args[i++]);
			templateStart = placeholderStart + 2;
		}
		builder.append(template.substring(templateStart));

		// if we run out of placeholders, append the extra args in square braces
		if (i < numArgs) {
			builder.append(" [");
			builder.append(args[i++]);
			while (i < numArgs) {
				builder.append(", ");
				builder.append(args[i++]);
			}
			builder.append(']');
		}

		return builder.toString();
	}
	
	// ------------------------------------------------------------------------

	/** Private constructor to prevent instantiation */
	private Preconditions() {}
}