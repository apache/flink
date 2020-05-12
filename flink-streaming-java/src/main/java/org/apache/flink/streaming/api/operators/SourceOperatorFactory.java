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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorProvider;

/**
 * The Factory class for {@link SourceOperator}.
 */
public class SourceOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT>
		implements CoordinatedOperatorFactory<OUT> {
	/** The {@link Source} to create the {@link SourceOperator}. */
	private final Source<OUT, ?, ?> source;
	/** The number of worker thread for the source coordinator. */
	private final int numCoordinatorWorkerThread;
	/** The {@link OperatorEventDispatcher} to register the SourceOperator. */
	private OperatorEventDispatcher operatorEventDispatcher;

	public SourceOperatorFactory(Source<OUT, ?, ?> source) {
		this(source, 1);
	}

	public SourceOperatorFactory(Source<OUT, ?, ?> source, int numCoordinatorWorkerThread) {
		this.source = source;
		this.numCoordinatorWorkerThread = numCoordinatorWorkerThread;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
		SourceOperator<OUT, ?> sourceOperator = new SourceOperator<>(source);
		OperatorEventGateway operatorEventGateway = operatorEventDispatcher.registerEventHandler(
				parameters.getStreamConfig().getOperatorID(),
				sourceOperator);
		sourceOperator.setOperatorEventGateway(operatorEventGateway);
		sourceOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		return (T) sourceOperator;
	}

	@Override
	public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
		return new SourceCoordinatorProvider<>(operatorName, operatorID, source, numCoordinatorWorkerThread);
	}

	@Override
	public void setOperatorEventDispatcher(OperatorEventDispatcher operatorEventDispatcher) {
		this.operatorEventDispatcher = operatorEventDispatcher;
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return SourceOperator.class;
	}

	@Override
	public boolean isStreamSource() {
		return true;
	}
}
