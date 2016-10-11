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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.Internal;

/**
 * Type extraction always contains some uncertainty due to unpredictable JVM differences
 * between vendors or versions. This exception is thrown if an assumption failed during extraction.
 */
@Internal
public class TypeExtractionException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new exception with no message.
	 */
	public TypeExtractionException() {
		super();
	}

	/**
	 * Creates a new exception with the given message.
	 *
	 * @param message The exception message.
	 */
	public TypeExtractionException(String message) {
		super(message);
	}

	/**
	 * Creates a new exception with the given message and cause.
	 *
	 * @param message The exception message.
	 * @param e cause
	 */
	public TypeExtractionException(String message, Throwable e) {
		super(message, e);
	}
}
