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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.runtime.state.StateHandle;

/**
 * This interface must be implemented by any invokable that has recoverable state.
 * The method {@link #setInitialState(org.apache.flink.runtime.state.StateHandle)} is used
 * to set the initial state of the operator, upon recovery.
 */
public interface OperatorStateCarrier<T extends StateHandle<?>> {

	/**
	 * Sets the initial state of the operator, upon recovery. The initial state is typically
	 * a snapshot of the state from a previous execution.
	 * 
	 * @param stateHandle The handle to the state.
	 */
	public void setInitialState(T stateHandle) throws Exception;

}
