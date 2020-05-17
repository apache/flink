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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.io.InputStatus;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.util.CollectionUtil;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base source operator only used for integrating the source reader which is proposed by FLIP-27. It implements
 * the interface of {@link PushingAsyncDataInput} for naturally compatible with one input processing in runtime
 * stack.
 *
 * <p><b>Important Note on Serialization:</b> The SourceOperator inherits the {@link java.io.Serializable}
 * interface from the StreamOperator, but is in fact NOT serializable. The operator must only be instantiates
 * in the StreamTask from its factory.
 *
 * @param <OUT> The output type of the operator.
 */
@Internal
@SuppressWarnings("serial")
public class SourceOperator<OUT, SplitT extends SourceSplit>
		extends AbstractStreamOperator<OUT>
		implements OperatorEventHandler, PushingAsyncDataInput<OUT> {
	private static final long serialVersionUID = 1405537676017904695L;

	// Package private for unit test.
	static final ListStateDescriptor<byte[]> SPLITS_STATE_DESC =
			new ListStateDescriptor<>("SourceReaderState", BytePrimitiveArraySerializer.INSTANCE);

	/** The factory for the source reader. This is a workaround, because currently the SourceReader
	 * must be lazily initialized, which is mainly because the metrics groups that the reader relies on is
	 * lazily initialized. */
	private final Function<SourceReaderContext, SourceReader<OUT, SplitT>> readerFactory;

	/** The serializer for the splits, applied to the split types before storing them in the reader state. */
	private final SimpleVersionedSerializer<SplitT> splitSerializer;

	/** The event gateway through which this operator talks to its coordinator. */
	private final OperatorEventGateway operatorEventGateway;

	// ---- lazily initialized fields ----

	/** The source reader that does most of the work. */
	private SourceReader<OUT, SplitT> sourceReader;

	/** The state that holds the currently assigned splits. */
	private ListState<SplitT> readerState;

	public SourceOperator(
			Function<SourceReaderContext, SourceReader<OUT, SplitT>> readerFactory,
			OperatorEventGateway operatorEventGateway,
			SimpleVersionedSerializer<SplitT> splitSerializer) {

		this.readerFactory = checkNotNull(readerFactory);
		this.operatorEventGateway = checkNotNull(operatorEventGateway);
		this.splitSerializer = checkNotNull(splitSerializer);
	}

	@Override
	public void open() throws Exception {
		final SourceReaderContext context = new SourceReaderContext() {
			@Override
			public MetricGroup metricGroup() {
				return getRuntimeContext().getMetricGroup();
			}

			@Override
			public void sendSourceEventToCoordinator(SourceEvent event) {
				operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
			}
		};

		sourceReader = readerFactory.apply(context);

		// restore the state if necessary.
		final List<SplitT> splits = CollectionUtil.iterableToList(readerState.get());
		if (!splits.isEmpty()) {
			sourceReader.addSplits(splits);
		}

		// Start the reader.
		sourceReader.start();
		// Register the reader to the coordinator.
		registerReader();
	}

	@Override
	@SuppressWarnings("unchecked")
	public InputStatus emitNext(DataOutput<OUT> output) throws Exception {
		switch (sourceReader.pollNext((SourceOutput<OUT>) output)) {
			case AVAILABLE_NOW:
				return InputStatus.MORE_AVAILABLE;
			case AVAILABLE_LATER:
				return InputStatus.NOTHING_AVAILABLE;
			case FINISHED:
				return InputStatus.END_OF_INPUT;
			default:
				throw new IllegalStateException("Should never reach here");
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		LOG.debug("Taking a snapshot for checkpoint {}", context.getCheckpointId());
		readerState.update(sourceReader.snapshotState());
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return sourceReader.isAvailable();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		final ListState<byte[]> rawState = context.getOperatorStateStore().getListState(SPLITS_STATE_DESC);
		readerState = new SimpleVersionedListState<>(rawState, splitSerializer);
	}

	@SuppressWarnings("unchecked")
	public void handleOperatorEvent(OperatorEvent event) {
		if (event instanceof AddSplitEvent) {
			sourceReader.addSplits(((AddSplitEvent<SplitT>) event).splits());
		} else if (event instanceof SourceEventWrapper) {
			sourceReader.handleSourceEvents(((SourceEventWrapper) event).getSourceEvent());
		} else {
			throw new IllegalStateException("Received unexpected operator event " + event);
		}
	}

	private void registerReader() {
		operatorEventGateway.sendEventToCoordinator(new ReaderRegistrationEvent(
				getRuntimeContext().getIndexOfThisSubtask(),
				"UNKNOWN_LOCATION"));
	}

	// --------------- methods for unit tests ------------

	@VisibleForTesting
	public SourceReader<OUT, SplitT> getSourceReader() {
		return sourceReader;
	}

	@VisibleForTesting
	ListState<SplitT> getReaderState() {
		return readerState;
	}
}
