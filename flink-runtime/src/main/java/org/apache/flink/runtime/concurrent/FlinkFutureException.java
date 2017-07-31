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

package org.apache.flink.runtime.concurrent;

import org.apache.flink.util.FlinkRuntimeException;

import java.util.concurrent.CompletionStage;

/**
 * Base class for exceptions which are thrown in {@link CompletionStage}.
 *
 * <p>The exception has to extend {@link FlinkRuntimeException} because only
 * unchecked exceptions can be thrown in a future's stage. Additionally we let
 * it extend the Flink runtime exception because it designates the exception to
 * come from a Flink stage.
 */
public class FlinkFutureException extends FlinkRuntimeException {
	private static final long serialVersionUID = -8878194471694178210L;

	public FlinkFutureException(String message) {
		super(message);
	}

	public FlinkFutureException(Throwable cause) {
		super(cause);
	}

	public FlinkFutureException(String message, Throwable cause) {
		super(message, cause);
	}
}
