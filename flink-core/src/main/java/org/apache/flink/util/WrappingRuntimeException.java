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

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A runtime exception that is explicitly used to wrap non-runtime exceptions.
 *
 * <p>The exception is recognized (for example by the Task when reporting exceptions as
 * failure causes) and unwrapped to avoid including the wrapper's stack trace in the reports.
 * That way, exception traces are keeping to the important parts.
 */
public class WrappingRuntimeException extends FlinkRuntimeException {

	private static final long serialVersionUID = 1L;

	public WrappingRuntimeException(@Nonnull Throwable cause) {
		super(checkNotNull(cause));
	}

	public WrappingRuntimeException(String message, @Nonnull Throwable cause) {
		super(message, checkNotNull(cause));
	}

	/**
	 * Recursively unwraps this WrappingRuntimeException and its causes, getting the first
	 * non wrapping exception.
	 *
	 * @return The first cause that is not a wrapping exception.
	 */
	public Throwable unwrap() {
		Throwable cause = getCause();
		return (cause instanceof WrappingRuntimeException) ? ((WrappingRuntimeException) cause).unwrap() : cause;
	}
}
