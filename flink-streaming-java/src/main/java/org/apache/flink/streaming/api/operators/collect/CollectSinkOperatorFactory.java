/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * The Factory class for {@link CollectSinkOperator}.
 */
@SuppressWarnings("unchecked")
public class CollectSinkOperatorFactory extends SimpleUdfStreamOperatorFactory<Object> implements CoordinatedOperatorFactory<Object> {

	private final CollectSinkOperator operator;

	private OperatorEventDispatcher operatorEventDispatcher;

	public CollectSinkOperatorFactory(CollectSinkOperator operator) {
		super(operator);
		this.operator = operator;
	}

	@Override
	public StreamOperator createStreamOperator(StreamOperatorParameters parameters) {
		OperatorEventGateway operatorEventGateway = operatorEventDispatcher.registerEventHandler(
			parameters.getStreamConfig().getOperatorID(),
			operator);
		operator.setOperatorEventGateway(operatorEventGateway);
		operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		return operator;
	}

	@Override
	public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
		operator.getOperatorIdFuture().complete(operatorID);
		return new CollectSinkOperatorCoordinator.Provider(operatorID);
	}

	@Override
	public void setOperatorEventDispatcher(OperatorEventDispatcher operatorEventDispatcher) {
		this.operatorEventDispatcher = operatorEventDispatcher;
	}
}
