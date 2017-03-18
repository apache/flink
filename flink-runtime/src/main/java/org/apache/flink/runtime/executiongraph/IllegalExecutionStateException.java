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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.execution.ExecutionState;

/**
 * A special {@link IllegalStateException} indicating a mismatch in the expected and actual
 * {@link ExecutionState} of an {@link Execution}.
 */
public class IllegalExecutionStateException extends IllegalStateException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new IllegalExecutionStateException with the error message indicating
	 * the expected and actual state.
	 * 
	 * @param expected The expected state 
	 * @param actual   The actual state
	 */
	public IllegalExecutionStateException(ExecutionState expected, ExecutionState actual) {
		super("Invalid execution state: Expected " + expected + " , found " + actual);
	}

	/**
	 * Creates a new IllegalExecutionStateException with the error message indicating
	 * the expected and actual state.
	 *
	 * @param expected The expected state 
	 * @param actual   The actual state
	 */
	public IllegalExecutionStateException(Execution execution, ExecutionState expected, ExecutionState actual) {
		super(execution.getVertexWithAttempt() + " is no longer in expected state " + expected + 
				" but in state " + actual);
	}
}
