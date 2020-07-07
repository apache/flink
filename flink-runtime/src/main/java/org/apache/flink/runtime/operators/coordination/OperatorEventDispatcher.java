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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;

/**
 * The dispatcher through which Operators receive {@link OperatorEvent}s and through which they can
 * send OperatorEvents back to the {@code OperatorCoordinator}.
 */
public interface OperatorEventDispatcher {

	/**
	 * Register a listener that is notified every time an OperatorEvent is sent from the
	 * OperatorCoordinator (of the operator with the given OperatorID) to this subtask.
	 */
	void registerEventHandler(OperatorID operator, OperatorEventHandler handler);

	/**
	 * Gets the gateway through which events can be passed to the OperatorCoordinator for
	 * the operator identified by the given OperatorID.
	 */
	OperatorEventGateway getOperatorEventGateway(OperatorID operatorId);
}
