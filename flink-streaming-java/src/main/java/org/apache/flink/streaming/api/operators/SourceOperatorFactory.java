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
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorProvider;

import java.util.function.Function;

/**
 * The Factory class for {@link SourceOperator}.
 */
public class SourceOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT>
		implements CoordinatedOperatorFactory<OUT> {

	private static final long serialVersionUID = 1L;

	/** The {@link Source} to create the {@link SourceOperator}. */
	private final Source<OUT, ?, ?> source;

	/** The number of worker thread for the source coordinator. */
	private final int numCoordinatorWorkerThread;

	public SourceOperatorFactory(Source<OUT, ?, ?> source) {
		this(source, 1);
	}

	public SourceOperatorFactory(Source<OUT, ?, ?> source, int numCoordinatorWorkerThread) {
		this.source = source;
		this.numCoordinatorWorkerThread = numCoordinatorWorkerThread;
	}

	@Override
	public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
		final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
		final OperatorEventGateway gateway = parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);

		final SourceOperator<OUT, ?> sourceOperator = instantiateSourceOperator(
				source::createReader,
				gateway,
				source.getSplitSerializer());

		sourceOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		parameters.getOperatorEventDispatcher().registerEventHandler(operatorId, sourceOperator);

		// today's lunch is generics spaghetti
		@SuppressWarnings("unchecked")
		final T castedOperator = (T) sourceOperator;

		return castedOperator;
	}

	@Override
	public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
		return new SourceCoordinatorProvider<>(operatorName, operatorID, source, numCoordinatorWorkerThread);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return SourceOperator.class;
	}

	@Override
	public boolean isStreamSource() {
		return true;
	}

	/**
	 * This is a utility method to conjure up a "SplitT" generics variable binding so that we can
	 * construct the SourceOperator without resorting to "all raw types".
	 * That way, this methods puts all "type non-safety" in one place and allows to maintain as much
	 * generics safety in the main code as possible.
	 */
	@SuppressWarnings("unchecked")
	private static <T, SplitT extends SourceSplit> SourceOperator<T, SplitT> instantiateSourceOperator(
			Function<SourceReaderContext, SourceReader<T, ?>> readerFactory,
			OperatorEventGateway eventGateway,
			SimpleVersionedSerializer<?> splitSerializer) {

		// jumping through generics hoops: cast the generics away to then cast them back more strictly typed
		final Function<SourceReaderContext, SourceReader<T, SplitT>> typedReaderFactory =
				(Function<SourceReaderContext, SourceReader<T, SplitT>>) (Function<?, ?>) readerFactory;

		final SimpleVersionedSerializer<SplitT> typedSplitSerializer = (SimpleVersionedSerializer<SplitT>) splitSerializer;

		return new SourceOperator<>(
				typedReaderFactory,
				eventGateway,
				typedSplitSerializer);
	}
}
