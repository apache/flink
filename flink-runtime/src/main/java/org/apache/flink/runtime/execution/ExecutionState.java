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

package org.apache.flink.runtime.execution;

/**
 * An enumeration of all states that a task can be in during its execution.
 * Tasks usually start in the state {@code CREATED} and switch states according to
 * this diagram:
 * <pre>
 *
 *     CREATED  -> SCHEDULED -> DEPLOYING -> RUNNING -> FINISHED
 *                     |            |          |
 *                     |            |   +------+
 *                     |            V   V
 *                     |         CANCELLING -----+----> CANCELED
 *                     |                         |
 *                     +-------------------------+
 *
 *                                               ... -> FAILED
 * </pre>
 *
 * <p>It is possible to enter the {@code FAILED} state from any other state.</p>
 *
 * <p>The states {@code FINISHED}, {@code CANCELED}, and {@code FAILED} are
 * considered terminal states.</p>
 */
public enum ExecutionState {

	CREATED,
	
	SCHEDULED,
	
	DEPLOYING,
	
	RUNNING,
	
	FINISHED,
	
	CANCELING,
	
	CANCELED,
	
	FAILED;


	public boolean isTerminal() {
		return this == FINISHED || this == CANCELED || this == FAILED;
	}
}
