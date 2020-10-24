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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
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
import org.apache.flink.streaming.api.operators.source.TimestampsAndWatermarks;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
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

	/** The factory for timestamps and watermark generators. */
	private final WatermarkStrategy<OUT> watermarkStrategy;

	/** The Flink configuration. */
	private final Configuration configuration;

	/** Host name of the machine where the operator runs, to support locality aware work assignment. */
	private final String localHostname;

	// ---- lazily initialized fields (these fields are the "hot" fields) ----

	/** The source reader that does most of the work. */
	private SourceReader<OUT, SplitT> sourceReader;

	private ReaderOutput<OUT> currentMainOutput;

	private DataOutput<OUT> lastInvokedOutput;

	/** The state that holds the currently assigned splits. */
	private ListState<SplitT> readerState;

	/** The event time and watermarking logic. Ideally this would be eagerly passed into this operator,
	 * but we currently need to instantiate this lazily, because the metric groups exist only later. */
	private TimestampsAndWatermarks<OUT> eventTimeLogic;

	public SourceOperator(
			Function<SourceReaderContext, SourceReader<OUT, SplitT>> readerFactory,
			OperatorEventGateway operatorEventGateway,
			SimpleVersionedSerializer<SplitT> splitSerializer,
			WatermarkStrategy<OUT> watermarkStrategy,
			ProcessingTimeService timeService,
			Configuration configuration,
			String localHostname) {

		this.readerFactory = checkNotNull(readerFactory);
		this.operatorEventGateway = checkNotNull(operatorEventGateway);
		this.splitSerializer = checkNotNull(splitSerializer);
		this.watermarkStrategy = checkNotNull(watermarkStrategy);
		this.processingTimeService = timeService;
		this.configuration = checkNotNull(configuration);
		this.localHostname = checkNotNull(localHostname);
	}

	@Override
	public void open() throws Exception {
		final MetricGroup metricGroup = getMetricGroup();

		final SourceReaderContext context = new SourceReaderContext() {
			@Override
			public MetricGroup metricGroup() {
				return metricGroup;
			}

			@Override
			public Configuration getConfiguration() {
				return configuration;
			}

			@Override
			public String getLocalHostName() {
				return localHostname;
			}

			@Override
			public void sendSourceEventToCoordinator(SourceEvent event) {
				operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
			}
		};

		// in the future when we support both batch and streaming modes for the source operator,
		// and when this one is migrated to the "eager initialization" operator (StreamOperatorV2),
		// then we should evaluate this during operator construction.
		eventTimeLogic = TimestampsAndWatermarks.createStreamingEventTimeLogic(
				watermarkStrategy,
				metricGroup,
				getProcessingTimeService(),
				getExecutionConfig().getAutoWatermarkInterval());

		sourceReader = readerFactory.apply(context);

		// restore the state if necessary.
		final List<SplitT> splits = CollectionUtil.iterableToList(readerState.get());
		if (!splits.isEmpty()) {
			sourceReader.addSplits(splits);
		}

		// Register the reader to the coordinator.
		registerReader();

		// Start the reader after registration, sending messages in start is allowed.
		sourceReader.start();

		eventTimeLogic.startPeriodicWatermarkEmits();
	}

	@Override
	public void close() throws Exception {
		sourceReader.close();
		eventTimeLogic.stopPeriodicWatermarkEmits();
		super.close();
	}

	@Override
	public InputStatus emitNext(DataOutput<OUT> output) throws Exception {
		// guarding an assumptions we currently make due to the fact that certain classes
		// assume a constant output
		assert lastInvokedOutput == output || lastInvokedOutput == null;

		// short circuit the common case (every invocation except the first)
		if (currentMainOutput != null) {
			return sourceReader.pollNext(currentMainOutput);
		}

		// this creates a batch or streaming output based on the runtime mode
		currentMainOutput = eventTimeLogic.createMainOutput(output);
		lastInvokedOutput = output;
		return sourceReader.pollNext(currentMainOutput);
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
			try {
				sourceReader.addSplits(((AddSplitEvent<SplitT>) event).splits(splitSerializer));
			} catch (IOException e) {
				throw new FlinkRuntimeException("Failed to deserialize the splits.", e);
			}
		} else if (event instanceof SourceEventWrapper) {
			sourceReader.handleSourceEvents(((SourceEventWrapper) event).getSourceEvent());
		} else {
			throw new IllegalStateException("Received unexpected operator event " + event);
		}
	}

	private void registerReader() {
		operatorEventGateway.sendEventToCoordinator(new ReaderRegistrationEvent(
				getRuntimeContext().getIndexOfThisSubtask(), localHostname));
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
