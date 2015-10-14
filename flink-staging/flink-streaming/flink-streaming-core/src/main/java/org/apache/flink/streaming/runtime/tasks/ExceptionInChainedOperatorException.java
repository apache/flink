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

package org.apache.flink.streaming.runtime.tasks;

import static java.util.Objects.requireNonNull;

/**
 * A special exception that signifies that the cause exception came from a chained operator.
 */
public class ExceptionInChainedOperatorException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ExceptionInChainedOperatorException(Throwable cause) {
		this("Could not forward element to next operator", cause);
	}

	public ExceptionInChainedOperatorException(String message, Throwable cause) {
		super(message, requireNonNull(cause));
	}
	
	public Throwable getOriginalCause() {
		Throwable ex = this;
		do {
			ex = ex.getCause();
		} while (ex instanceof ExceptionInChainedOperatorException);
		return ex; 
	}
}
