/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;

/**
 * A factory class for the {@link StreamOperator}s implementing
 * {@link org.apache.flink.runtime.operators.coordination.OperatorEventHandler}.
 */
public interface CoordinatedOperatorFactory<OUT> extends StreamOperatorFactory<OUT> {

	/**
	 * The implementation should return an instance of
	 * {@link org.apache.flink.runtime.operators.coordination.OperatorEventHandler}.
	 */
	@Override
	<T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters);

	/**
	 * Get the operator coordinator provider for this operator.
	 *
	 * @param operatorName the name of the operator.
	 * @param operatorID the id of the operator.
	 * @return the provider of the {@link OperatorCoordinator} for this operator.
	 */
	OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID);

	/**
	 * Sets the {@link OperatorEventDispatcher} for registering the
	 * {@link org.apache.flink.runtime.operators.coordination.OperatorEventHandler OperaterEventHandler} and setup the
	 * {@link org.apache.flink.runtime.operators.coordination.OperatorEventGateway OperatorEventGateway} for the
	 * SourceOperator to send events to the operator coordinator. This method will be invoked before
	 * {@link #createStreamOperator(StreamOperatorParameters)} is invoked.
	 *
	 * @param operatorEventDispatcher the {@link OperatorEventDispatcher} to register the
	 */
	void setOperatorEventDispatcher(OperatorEventDispatcher operatorEventDispatcher);
}
