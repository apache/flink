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

package org.apache.flink.util;

import org.apache.flink.annotation.Public;

/**
 * An exception that is thrown if the dynamic instantiation of code fails.
 *
 * <p>This exception is supposed to "sum up" the zoo of exceptions typically thrown around
 * dynamic code loading and instantiations:
 *
 * <pre>{@code
 * try {
 *     Class.forName(classname).asSubclass(TheType.class).newInstance();
 * }
 * catch (ClassNotFoundException | ClassCastException | InstantiationException | IllegalAccessException e) {
 *     throw new DynamicCodeLoadingException("Could not load and instantiate " + classname", e);
 * }
 * }</pre>
 */
@Public
public class DynamicCodeLoadingException extends FlinkException {

	private static final long serialVersionUID = -25138443817255490L;

	/**
	 * Creates a new exception with the given cause.
	 *
	 * @param cause The exception that caused this exception
	 */
	public DynamicCodeLoadingException(Throwable cause) {
		super(cause);
	}

	/**
	 * Creates a new exception with the given message and cause.
	 *
	 * @param message The exception message
	 * @param cause The exception that caused this exception
	 */
	public DynamicCodeLoadingException(String message, Throwable cause) {
		super(message, cause);
	}
}
