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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.collector.selector.CopyingDirectedOutput;
import org.apache.flink.streaming.api.collector.selector.DirectedOutput;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamSourceV2;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusProvider;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.XORShiftRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@code OperatorChain} contains all operators that are executed as one chain within a single
 * {@link StreamTask}.
 */
@Internal
public class OperatorChain implements StreamStatusMaintainer, InputSelector {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorChain.class);

	private final AbstractInvokable containingTask;

	private final Map<Integer, AbstractStreamOperatorProxy<?>> allOperators;

	private final Deque<StreamOperator<?>> allOperatorsTopologySorted;

	private final RecordWriterOutput<?>[] streamOutputs;

	private final Map<Integer, WatermarkGaugeExposingOutput<StreamRecord<?>>> chainEntryPoints = new HashMap<>();

	private final Map<Integer, StreamOperator> headOperators = new HashMap<>();

	private final Map<Integer, AbstractStreamOperatorProxy<?>> sourceHeadOperators = new HashMap<>();

	private final StreamTaskConfigSnapshot streamTaskConfig;

	/**
	 * Current status of the input stream of the operator chain.
	 * Watermarks explicitly generated by operators in the chain (i.e. timestamp
	 * assigner / watermark extractors), will be blocked and not forwarded if
	 * this value is {@link StreamStatus#IDLE}.
	 */
	private StreamStatus streamStatus = StreamStatus.ACTIVE;

	public OperatorChain(
			StreamTask containingTask,
			List<StreamRecordWriter<StreamRecord<?>>> streamRecordWriters) {

		this.containingTask = containingTask;

		final ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
		streamTaskConfig = containingTask.getStreamTaskConfig();

		// we read the chained configs, and the order of record writer registrations by output name
		Map<Integer, StreamConfig> chainedConfigs = streamTaskConfig.getChainedNodeConfigs();

		// create the final output stream writers
		// we iterate through all the out edges from this job vertex and create a stream output
		List<StreamEdge> outEdgesInOrder = streamTaskConfig.getOutStreamEdgesOfChain();
		Map<StreamEdge, RecordWriterOutput<?>> streamOutputMap = new HashMap<>(outEdgesInOrder.size());
		this.streamOutputs = new RecordWriterOutput<?>[outEdgesInOrder.size()];

		// from here on, we need to make sure that the output writers are shut down again on failure
		boolean success = false;
		try {
			for (int i = 0; i < outEdgesInOrder.size(); i++) {
				StreamEdge outEdge = outEdgesInOrder.get(i);

				RecordWriterOutput<?> streamOutput = createStreamOutput(
					streamRecordWriters.get(i),
					outEdge,
					chainedConfigs.get(outEdge.getSourceId()),
					containingTask.getEnvironment(),
					i);

				this.streamOutputs[i] = streamOutput;
				streamOutputMap.put(outEdge, streamOutput);
			}

			// we create the chain of operators and grab the collector that leads into the chain
			final Map<Integer, AbstractStreamOperatorProxy<?>> allOps = new HashMap<>(chainedConfigs.size());

			List<Integer> headIds = streamTaskConfig.getChainedHeadNodeIds();
			Preconditions.checkNotNull(headIds);

			if (!chainedConfigs.isEmpty()) {
				for (int headId : headIds) {
					if (!allOps.containsKey(headId)) {
						Tuple2<WatermarkGaugeExposingOutput, List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>>> outputAndSuccessors =
							createOutputCollector(
								containingTask,
								chainedConfigs.get(headId),
								chainedConfigs,
								userCodeClassloader,
								streamOutputMap,
								allOps);

						WatermarkGaugeExposingOutput output = outputAndSuccessors.f0;
						List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>> successors = outputAndSuccessors.f1;

						StreamOperator originalHeadOperator = chainedConfigs.get(headId).getStreamOperator(
							userCodeClassloader);

						// There might be a null head operator in iteration mode.
						if (originalHeadOperator != null) {

							AbstractStreamOperatorProxy headOperator = AbstractStreamOperatorProxy.proxy(originalHeadOperator, successors);
							//noinspection unchecked
							headOperator.setup(containingTask, chainedConfigs.get(headId), output);
							headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, output.getWatermarkGauge());

							headOperators.put(headId, originalHeadOperator);
							if (originalHeadOperator instanceof StreamSource || originalHeadOperator instanceof StreamSourceV2) {
								sourceHeadOperators.put(headId, headOperator);
							}
							allOps.put(headId, headOperator);
							chainEntryPoints.put(headId, output);
						}
					} else {
						headOperators.put(headId, allOps.get(headId).getOperator());
					}
				}
			}

			// There might be an empty allOps and some in edges in iteration mode.
			if (!allOps.isEmpty()) {
				for (StreamEdge streamEdge : streamTaskConfig.getInStreamEdgesOfChain()) {
					allOps.get(streamEdge.getTargetId()).addInputEdge(streamEdge);
				}
			}

			final Map<Integer, StreamOperator<?>> originalOperators = new HashMap<>();
			for (Map.Entry<Integer, AbstractStreamOperatorProxy<?>> entry : allOps.entrySet()) {
				originalOperators.put(entry.getKey(), entry.getValue().getOperator());
			}

			// Here we got all inputs inside the chain, get the topology sorted operators.
			allOperatorsTopologySorted = getTopologySortedOperators(headIds, userCodeClassloader, originalOperators, chainedConfigs);

			this.allOperators = allOps;

			success = true;
		}
		finally {
			// make sure we clean up after ourselves in case of a failure after acquiring
			// the first resources
			if (!success) {
				for (RecordWriterOutput<?> output : this.streamOutputs) {
					if (output != null) {
						output.close();
					}
				}
			}
		}

	}

	@Override
	public StreamStatus getStreamStatus() {
		return streamStatus;
	}

	@Override
	public void toggleStreamStatus(StreamStatus status) {
		if (!status.equals(this.streamStatus)) {
			this.streamStatus = status;

			// try and forward the stream status change to all outgoing connections
			for (RecordWriterOutput<?> streamOutput : streamOutputs) {
				streamOutput.emitStreamStatus(status);
			}
		}
	}

	public void broadcastCheckpointBarrier(long id, long timestamp, CheckpointOptions checkpointOptions) throws IOException {
		try {
			CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp, checkpointOptions);
			for (RecordWriterOutput<?> streamOutput : streamOutputs) {
				streamOutput.broadcastEvent(barrier);
			}
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted while broadcasting checkpoint barrier");
		}
	}

	public void broadcastCheckpointCancelMarker(long id) throws IOException {
		try {
			CancelCheckpointMarker barrier = new CancelCheckpointMarker(id);
			for (RecordWriterOutput<?> streamOutput : streamOutputs) {
				streamOutput.broadcastEvent(barrier);
			}
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted while broadcasting checkpoint cancellation");
		}
	}

	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		// go forward through the operator chain and tell each operator
		// to prepare the checkpoint
		// NOTE: prepareSnapshotPreBarrier must use ascend order iterator.
		for (StreamOperator<?> op : getAllOperatorsTopologySorted()) {
			op.prepareSnapshotPreBarrier(checkpointId);
		}
	}

	public RecordWriterOutput<?>[] getStreamOutputs() {
		return streamOutputs;
	}

	public Deque<StreamOperator<?>> getAllOperatorsTopologySorted() {
		return allOperatorsTopologySorted;
	}

	public Output<StreamRecord<?>>[] getChainEntryPoints() {
		//noinspection unchecked
		return chainEntryPoints.values().toArray(new Output[0]);
	}

	/**
	 * This method should be called before finishing the record emission, to make sure any data
	 * that is still buffered will be sent. It also ensures that all data sending related
	 * exceptions are recognized.
	 *
	 * @throws IOException Thrown, if the buffered data cannot be pushed into the output streams.
	 */
	public void flushOutputs() throws IOException {
		for (RecordWriterOutput<?> streamOutput : getStreamOutputs()) {
			streamOutput.flush();
		}
	}

	/**
	 * This method releases all resources of the record writer output. It stops the output
	 * flushing thread (if there is one) and releases all buffers currently held by the output
	 * serializers.
	 *
	 * <p>This method should never fail.
	 */
	public void releaseOutputs() {
		for (RecordWriterOutput<?> streamOutput : streamOutputs) {
			streamOutput.close();
		}
	}

	public StreamOperator[] getHeadOperators() {
		//noinspection unchecked
		return headOperators.values().toArray(new StreamOperator[0]);
	}

	public StreamOperator getHeadOperator(int headNodeId) {
		return headOperators.get(headNodeId);
	}

	public AbstractStreamOperatorProxy getOperatorProxy(int nodeId) {
		return allOperators.get(nodeId);
	}

	public int getChainLength() {
		return allOperators == null ? 0 : allOperators.size();
	}

	@Override
	public void registerSelectionChangedListener(SelectionChangedListener listener) {
		for (AbstractStreamOperatorProxy operatorProxy : allOperators.values()) {
			if (operatorProxy instanceof TwoInputStreamOperatorProxy) {
				((TwoInputStreamOperatorProxy) operatorProxy).registerSelectionChangedListener(listener);
			}
		}
	}

	public List<InputSelection> getNextSelectedInputs() {
		Map<StreamEdge, Boolean> visited = null;
		// If the DAG in chain is not large enough, we should skip checking duplicated visit
		if (allOperators.size() > 16) {
			visited = new HashMap<>(allOperators.size());
		}
		final List<InputSelection> selectedInputs = new ArrayList<>();

		for (StreamEdge inEdge : streamTaskConfig.getInStreamEdgesOfChain()) {
			if (allOperators.get(inEdge.getTargetId()).isSelected(inEdge, visited)) {
				selectedInputs.add(EdgeInputSelection.create(inEdge));
			}
		}

		for (Map.Entry<Integer, AbstractStreamOperatorProxy<?>> entry : sourceHeadOperators.entrySet()) {
			if (entry.getValue().isSelected(null, visited)) {
				selectedInputs.add(SourceInputSelection.create(entry.getKey()));
			}
		}

		return selectedInputs;
	}
	// ------------------------------------------------------------------------
	//  initialization utilities
	// ------------------------------------------------------------------------

	private <T> Tuple2<WatermarkGaugeExposingOutput, List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>>> createOutputCollector(
			StreamTask<?, ?> containingTask,
			StreamConfig operatorConfig,
			Map<Integer, StreamConfig> chainedConfigs,
			ClassLoader userCodeClassloader,
			Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
			Map<Integer, AbstractStreamOperatorProxy<?>> allOperators) {
		List<Tuple2<WatermarkGaugeExposingOutput<StreamRecord<T>>, StreamEdge>> allOutputs = new ArrayList<>(4);
		List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>> allSuccessors = new ArrayList<>(4);

		// create collectors for the network outputs
		for (StreamEdge outputEdge : operatorConfig.getNonChainedOutputs(userCodeClassloader)) {
			@SuppressWarnings("unchecked")
			RecordWriterOutput<T> output = (RecordWriterOutput<T>) streamOutputs.get(outputEdge);

			allOutputs.add(new Tuple2<>(output, outputEdge));
		}

		// Create collectors for the chained outputs
		for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {
			int outputId = outputEdge.getTargetId();
			StreamConfig chainedOpConfig = chainedConfigs.get(outputId);

			WatermarkGaugeExposingOutput<StreamRecord<T>> output = createChainedOperator(
				containingTask,
				chainedOpConfig,
				chainedConfigs,
				userCodeClassloader,
				streamOutputs,
				allOperators,
				outputEdge);
			allOutputs.add(new Tuple2<>(output, outputEdge));
			allSuccessors.add(Tuple2.of(allOperators.get(outputId), outputEdge));
		}

		// if there are multiple outputs, or the outputs are directed, we need to
		// wrap them as one output

		List<OutputSelector<T>> selectors = operatorConfig.getOutputSelectors(userCodeClassloader);

		if (selectors == null || selectors.isEmpty()) {
			// simple path, no selector necessary
			if (allOutputs.size() == 1) {
				return Tuple2.of(allOutputs.get(0).f0, allSuccessors);
			}
			else {
				// send to N outputs. Note that this includes teh special case
				// of sending to zero outputs
				@SuppressWarnings({"unchecked", "rawtypes"})
				Output<StreamRecord<T>>[] asArray = new Output[allOutputs.size()];
				for (int i = 0; i < allOutputs.size(); i++) {
					asArray[i] = allOutputs.get(i).f0;
				}

				// This is the inverse of creating the normal ChainingOutput.
				// If the chaining output does not copy we need to copy in the broadcast output,
				// otherwise multi-chaining would not work correctly.
				if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
					return Tuple2.of(new CopyingBroadcastingOutputCollector<>(asArray, this), allSuccessors);
				} else {
					return Tuple2.of(new BroadcastingOutputCollector<>(asArray, this), allSuccessors);
				}
			}
		}
		else {
			// selector present, more complex routing necessary

			// This is the inverse of creating the normal ChainingOutput.
			// If the chaining output does not copy we need to copy in the broadcast output,
			// otherwise multi-chaining would not work correctly.
			if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
				return Tuple2.of(new CopyingDirectedOutput<>(selectors, allOutputs), allSuccessors);
			} else {
				return Tuple2.of(new DirectedOutput<>(selectors, allOutputs), allSuccessors);
			}

		}
	}

	private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> createChainedOperator(
			StreamTask<?, ?> containingTask,
			StreamConfig operatorConfig,
			Map<Integer, StreamConfig> chainedConfigs,
			ClassLoader userCodeClassloader,
			Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
			Map<Integer, AbstractStreamOperatorProxy<?>> allOperators,
			StreamEdge inputEdge) {

		// This chained operator may has been traversed from the other edge
		@SuppressWarnings("unchecked")
		AbstractStreamOperatorProxy<OUT> chainedOperator = (AbstractStreamOperatorProxy<OUT>) allOperators.get(inputEdge.getTargetId());

		if (chainedOperator == null) {
			// create the output that the operator writes to first. this may recursively create more operators
			Tuple2<WatermarkGaugeExposingOutput, List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>>> outputAndSuccessors = createOutputCollector(
				containingTask,
				operatorConfig,
				chainedConfigs,
				userCodeClassloader,
				streamOutputs,
				allOperators);

			//noinspection unchecked
			WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput = outputAndSuccessors.f0;
			List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>> successors = outputAndSuccessors.f1;

			// now create the operator and give it the output collector to write its output to
			//noinspection unchecked
			chainedOperator = AbstractStreamOperatorProxy.proxy(operatorConfig.getStreamOperator(userCodeClassloader), successors);

			chainedOperator.setup(containingTask, operatorConfig, chainedOperatorOutput);

			allOperators.put(inputEdge.getTargetId(), chainedOperator);

			chainedOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, chainedOperatorOutput.getWatermarkGauge()::getValue);
		}

		chainedOperator.addInputEdge(inputEdge);

		 // 1. Output to the origin operator when it's an OneInputStreamOperator
		 // 2. Output to the proxy operator when it's a TwoInputStreamOperator
		 // 3. Fork multiple output class to reduce cost of per record processing introduced by inheritance
		WatermarkGaugeExposingOutput<StreamRecord<IN>> currentOperatorOutput;
		if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
			if (chainedOperator instanceof OneInputStreamOperator) {
				currentOperatorOutput = new ChainingWithOneInputStreamOperatorOutput<>((OneInputStreamOperator<IN, ?>) chainedOperator.getOperator(), this, inputEdge);
			} else if (chainedOperator instanceof TwoInputStreamOperator) {
				if (inputEdge.getTypeNumber() == 1) {
					currentOperatorOutput = new ChainingWithFirstInputOfTwoInputStreamOperatorOutput<>((TwoInputStreamOperator<IN, ?, ?>) chainedOperator, this, inputEdge);
				} else if (inputEdge.getTypeNumber() == 2) {
					currentOperatorOutput = new ChainingWithSecondInputOfTwoInputStreamOperatorOutput<>((TwoInputStreamOperator<?, IN, ?>) chainedOperator, this, inputEdge);
				} else {
					throw new RuntimeException("Unexpected type number of edge " + inputEdge);
				}
			} else {
				throw new RuntimeException("Unexpected operator type " + chainedOperator.getOperator());
			}
		}
		else {
			final TypeSerializer<IN> inSerializer;
			if (inputEdge.getTypeNumber() == 2) {
				inSerializer = operatorConfig.getTypeSerializerIn2(userCodeClassloader);
			} else {
				inSerializer = operatorConfig.getTypeSerializerIn1(userCodeClassloader);
			}
			if (chainedOperator instanceof OneInputStreamOperator) {
				currentOperatorOutput = new CopyingChainingWithOneInputStreamOperatorOutput<>((OneInputStreamOperator<IN, ?>) chainedOperator.getOperator(), inSerializer, inputEdge, this);
			} else if (chainedOperator instanceof TwoInputStreamOperator) {
				if (inputEdge.getTypeNumber() == 1) {
					currentOperatorOutput = new CopyingChainingWithFirstInputOfTwoInputStreamOperatorOutput<>((TwoInputStreamOperator<IN, ?, ?>) chainedOperator, inSerializer, inputEdge, this);
				} else if (inputEdge.getTypeNumber() == 2) {
					currentOperatorOutput = new CopyingChainingWithSecondInputOfTwoInputStreamOperatorOutput<>((TwoInputStreamOperator<?, IN, ?>) chainedOperator, inSerializer, inputEdge, this);
				} else {
					throw new RuntimeException("Unexpected type number of edge " + inputEdge);
				}
			} else {
				throw new RuntimeException("Unexpected operator type " + chainedOperator.getOperator());
			}
		}

		chainedOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, currentOperatorOutput.getWatermarkGauge()::getValue);

		return currentOperatorOutput;
	}

	private RecordWriterOutput<?> createStreamOutput(
			StreamRecordWriter<StreamRecord<?>> streamRecordWriter,
			StreamEdge edge,
			StreamConfig upStreamConfig,
			Environment taskEnvironment,
			int outputIndex) {
		OutputTag sideOutputTag = edge.getOutputTag(); // OutputTag, return null if not sideOutput

		TypeSerializer outSerializer = null;

		if (edge.getOutputTag() != null) {
			// side output
			outSerializer = upStreamConfig.getTypeSerializerSideOut(
					edge.getOutputTag(), taskEnvironment.getUserClassLoader());
		} else {
			// main output
			outSerializer = upStreamConfig.getTypeSerializerOut(taskEnvironment.getUserClassLoader());
		}

		TypeSerializer<StreamElement> outRecordSerializer = new StreamElementSerializer<>(outSerializer);
		taskEnvironment.getWriter(outputIndex).setTypeSerializer(outRecordSerializer);
		taskEnvironment.getWriter(outputIndex).setParentTask(containingTask);

		return new RecordWriterOutput(streamRecordWriter, sideOutputTag, this);
	}

	// ------------------------------------------------------------------------
	//  Collectors for output chaining
	// ------------------------------------------------------------------------

	/**
	 * An {@link Output} that measures the last emitted watermark with a {@link WatermarkGauge}.
	 *
	 * @param <T> The type of the elements that can be emitted.
	 */
	public interface WatermarkGaugeExposingOutput<T> extends Output<T> {
		Gauge<Long> getWatermarkGauge();
	}

	private static class ChainingWithOneInputStreamOperatorOutput<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {

		protected final OneInputStreamOperator<T, ?> operator;
		protected final Counter numRecordsIn;
		protected final WatermarkGauge watermarkGauge = new WatermarkGauge();

		protected final StreamStatusProvider streamStatusProvider;

		protected final OutputTag<T> outputTag;

		public ChainingWithOneInputStreamOperatorOutput(
				OneInputStreamOperator<T, ?> operator,
				StreamStatusProvider streamStatusProvider,
				StreamEdge edge) {

			this.operator = operator;
			Counter tmpNumRecordsIn;
			try {
				OperatorIOMetricGroup ioMetricGroup = ((OperatorMetricGroup) operator.getMetricGroup()).getIOMetricGroup();
				tmpNumRecordsIn = ioMetricGroup.getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				tmpNumRecordsIn = new SimpleCounter();
			}
			numRecordsIn = tmpNumRecordsIn;

			this.streamStatusProvider = streamStatusProvider;
			this.outputTag = edge.getOutputTag();
		}

		@Override
		public void collect(StreamRecord<T> record) {
			if (this.outputTag != null) {
				// we are only responsible for emitting to the main input
				return;
			}

			pushToOperator(record);
		}

		@Override
		public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
			if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
				// we are only responsible for emitting to the side-output specified by our
				// OutputTag.
				return;
			}

			pushToOperator(record);
		}

		protected <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator expects.
				@SuppressWarnings("unchecked")
				final StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				operator.setKeyContextElement1(castRecord);
				operator.processElement(castRecord);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			try {
				watermarkGauge.setCurrentWatermark(mark.getTimestamp());
				if (streamStatusProvider.getStreamStatus().isActive()) {
					operator.processWatermark(mark);
				}
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			try {
				operator.processLatencyMarker(latencyMarker);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void close() {
			try {
				operator.close();
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public Gauge<Long> getWatermarkGauge() {
			return watermarkGauge;
		}
	}

	private static class ChainingWithFirstInputOfTwoInputStreamOperatorOutput<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {

		protected final TwoInputStreamOperator<T, ?, ?> operator;
		protected final Counter numRecordsIn;
		protected final WatermarkGauge watermarkGauge = new WatermarkGauge();

		protected final StreamStatusProvider streamStatusProvider;

		protected final OutputTag<T> outputTag;

		public ChainingWithFirstInputOfTwoInputStreamOperatorOutput(
			TwoInputStreamOperator<T, ?, ?> operator,
			StreamStatusProvider streamStatusProvider,
			StreamEdge edge) {

			this.operator = operator;
			Counter tmpNumRecordsIn;
			try {
				OperatorIOMetricGroup ioMetricGroup = ((OperatorMetricGroup) operator.getMetricGroup()).getIOMetricGroup();
				tmpNumRecordsIn = ioMetricGroup.getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				tmpNumRecordsIn = new SimpleCounter();
			}
			numRecordsIn = tmpNumRecordsIn;

			this.streamStatusProvider = streamStatusProvider;
			this.outputTag = edge.getOutputTag();
		}

		@Override
		public void collect(StreamRecord<T> record) {
			if (this.outputTag != null) {
				// we are only responsible for emitting to the main input
				return;
			}

			pushToOperator(record);
		}

		@Override
		public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
			if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
				// we are only responsible for emitting to the side-output specified by our
				// OutputTag.
				return;
			}

			pushToOperator(record);
		}

		protected <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator expects.
				@SuppressWarnings("unchecked")
				final StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				operator.setKeyContextElement1(castRecord);
				operator.processElement1(castRecord);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			try {
				watermarkGauge.setCurrentWatermark(mark.getTimestamp());
				if (streamStatusProvider.getStreamStatus().isActive()) {
					operator.processWatermark1(mark);
				}
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			try {
				operator.processLatencyMarker2(latencyMarker);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void close() {
			try {
				operator.close();
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public Gauge<Long> getWatermarkGauge() {
			return watermarkGauge;
		}
	}

	private static class ChainingWithSecondInputOfTwoInputStreamOperatorOutput<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {

		protected final TwoInputStreamOperator<?, T, ?> operator;
		protected final Counter numRecordsIn;
		protected final WatermarkGauge watermarkGauge = new WatermarkGauge();

		protected final StreamStatusProvider streamStatusProvider;

		protected final OutputTag<T> outputTag;

		public ChainingWithSecondInputOfTwoInputStreamOperatorOutput(
			TwoInputStreamOperator<?, T, ?> operator,
			StreamStatusProvider streamStatusProvider,
			StreamEdge edge) {

			this.operator = operator;
			Counter tmpNumRecordsIn;
			try {
				OperatorIOMetricGroup ioMetricGroup = ((OperatorMetricGroup) operator.getMetricGroup()).getIOMetricGroup();
				tmpNumRecordsIn = ioMetricGroup.getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				tmpNumRecordsIn = new SimpleCounter();
			}
			numRecordsIn = tmpNumRecordsIn;

			this.streamStatusProvider = streamStatusProvider;
			this.outputTag = edge.getOutputTag();
		}

		@Override
		public void collect(StreamRecord<T> record) {
			if (this.outputTag != null) {
				// we are only responsible for emitting to the main input
				return;
			}

			pushToOperator(record);
		}

		@Override
		public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
			if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
				// we are only responsible for emitting to the side-output specified by our
				// OutputTag.
				return;
			}

			pushToOperator(record);
		}

		protected <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator expects.
				@SuppressWarnings("unchecked")
				final StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				operator.setKeyContextElement2(castRecord);
				operator.processElement2(castRecord);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			try {
				watermarkGauge.setCurrentWatermark(mark.getTimestamp());
				if (streamStatusProvider.getStreamStatus().isActive()) {
					operator.processWatermark2(mark);
				}
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			try {
				operator.processLatencyMarker2(latencyMarker);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void close() {
			try {
				operator.close();
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public Gauge<Long> getWatermarkGauge() {
			return watermarkGauge;
		}
	}

	private static final class CopyingChainingWithOneInputStreamOperatorOutput<T> extends ChainingWithOneInputStreamOperatorOutput<T> {

		private final TypeSerializer<T> serializer;

		public CopyingChainingWithOneInputStreamOperatorOutput(
				OneInputStreamOperator<T, ?> operator,
				TypeSerializer<T> serializer,
				StreamEdge edge,
				StreamStatusProvider streamStatusProvider) {
			super(operator, streamStatusProvider, edge);
			this.serializer = checkNotNull(serializer);
		}

		@Override
		protected final <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator (and Serializer) expects.
				@SuppressWarnings("unchecked")
				final StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				final StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
				operator.setKeyContextElement1(copy);
				operator.processElement(copy);
			} catch (ClassCastException e) {
				if (outputTag != null) {
					// Enrich error message
					ClassCastException replace = new ClassCastException(
						String.format(
							"%s. Failed to push OutputTag with id '%s' to operator. " +
								"This can occur when multiple OutputTags with different types " +
								"but identical names are being used.",
							e.getMessage(),
							outputTag.getId()));

					throw new ExceptionInChainedOperatorException(replace);
				} else {
					throw new ExceptionInChainedOperatorException(e);
				}
			} catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}

		}
	}

	private static final class CopyingChainingWithFirstInputOfTwoInputStreamOperatorOutput<T> extends ChainingWithFirstInputOfTwoInputStreamOperatorOutput<T> {

		private final TypeSerializer<T> serializer;

		public CopyingChainingWithFirstInputOfTwoInputStreamOperatorOutput(
			TwoInputStreamOperator<T, ?, ?> operator,
			TypeSerializer<T> serializer,
			StreamEdge edge,
			StreamStatusProvider streamStatusProvider) {
			super(operator, streamStatusProvider, edge);
			this.serializer = checkNotNull(serializer);
		}

		@Override
		protected final <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator (and Serializer) expects.
				@SuppressWarnings("unchecked")
				final StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				final StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
				operator.setKeyContextElement1(copy);
				operator.processElement1(copy);
			} catch (ClassCastException e) {
				if (outputTag != null) {
					// Enrich error message
					ClassCastException replace = new ClassCastException(
						String.format(
							"%s. Failed to push OutputTag with id '%s' to operator. " +
								"This can occur when multiple OutputTags with different types " +
								"but identical names are being used.",
							e.getMessage(),
							outputTag.getId()));

					throw new ExceptionInChainedOperatorException(replace);
				} else {
					throw new ExceptionInChainedOperatorException(e);
				}
			} catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}

		}
	}

	private static final class CopyingChainingWithSecondInputOfTwoInputStreamOperatorOutput<T> extends ChainingWithSecondInputOfTwoInputStreamOperatorOutput<T> {

		private final TypeSerializer<T> serializer;

		public CopyingChainingWithSecondInputOfTwoInputStreamOperatorOutput(
			TwoInputStreamOperator<?, T, ?> operator,
			TypeSerializer<T> serializer,
			StreamEdge edge,
			StreamStatusProvider streamStatusProvider) {
			super(operator, streamStatusProvider, edge);
			this.serializer = checkNotNull(serializer);
		}

		@Override
		protected final <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator (and Serializer) expects.
				@SuppressWarnings("unchecked")
				final StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				final StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
				operator.setKeyContextElement2(copy);
				operator.processElement2(copy);
			} catch (ClassCastException e) {
				if (outputTag != null) {
					// Enrich error message
					ClassCastException replace = new ClassCastException(
						String.format(
							"%s. Failed to push OutputTag with id '%s' to operator. " +
								"This can occur when multiple OutputTags with different types " +
								"but identical names are being used.",
							e.getMessage(),
							outputTag.getId()));

					throw new ExceptionInChainedOperatorException(replace);
				} else {
					throw new ExceptionInChainedOperatorException(e);
				}
			} catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}

		}
	}

	private static class BroadcastingOutputCollector<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {

		protected final Output<StreamRecord<T>>[] outputs;

		private final Random random = new XORShiftRandom();

		private final StreamStatusProvider streamStatusProvider;

		private final WatermarkGauge watermarkGauge = new WatermarkGauge();

		public BroadcastingOutputCollector(
				Output<StreamRecord<T>>[] outputs,
				StreamStatusProvider streamStatusProvider) {
			this.outputs = outputs;
			this.streamStatusProvider = streamStatusProvider;
		}

		@Override
		public void emitWatermark(Watermark mark) {
			watermarkGauge.setCurrentWatermark(mark.getTimestamp());
			if (streamStatusProvider.getStreamStatus().isActive()) {
				for (Output<StreamRecord<T>> output : outputs) {
					output.emitWatermark(mark);
				}
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			if (outputs.length <= 0) {
				// ignore
			} else if (outputs.length == 1) {
				outputs[0].emitLatencyMarker(latencyMarker);
			} else {
				// randomly select an output
				outputs[random.nextInt(outputs.length)].emitLatencyMarker(latencyMarker);
			}
		}

		@Override
		public Gauge<Long> getWatermarkGauge() {
			return watermarkGauge;
		}

		@Override
		public void collect(StreamRecord<T> record) {
			for (Output<StreamRecord<T>> output : outputs) {
				output.collect(record);
			}
		}

		@Override
		public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
			for (Output<StreamRecord<T>> output : outputs) {
				output.collect(outputTag, record);
			}
		}

		@Override
		public void close() {
			for (Output<StreamRecord<T>> output : outputs) {
				output.close();
			}
		}
	}

	/**
	 * Special version of {@link BroadcastingOutputCollector} that performs a shallow copy of the
	 * {@link StreamRecord} to ensure that multi-chaining works correctly.
	 */
	private static final class CopyingBroadcastingOutputCollector<T> extends BroadcastingOutputCollector<T> {

		public CopyingBroadcastingOutputCollector(
				Output<StreamRecord<T>>[] outputs,
				StreamStatusProvider streamStatusProvider) {
			super(outputs, streamStatusProvider);
		}

		@Override
		public void collect(StreamRecord<T> record) {

			for (int i = 0; i < outputs.length - 1; i++) {
				Output<StreamRecord<T>> output = outputs[i];
				StreamRecord<T> shallowCopy = record.copy(record.getValue());
				output.collect(shallowCopy);
			}

			if (outputs.length > 0) {
				// don't copy for the last output
				outputs[outputs.length - 1].collect(record);
			}
		}

		@Override
		public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
			for (int i = 0; i < outputs.length - 1; i++) {
				Output<StreamRecord<T>> output = outputs[i];

				StreamRecord<X> shallowCopy = record.copy(record.getValue());
				output.collect(outputTag, shallowCopy);
			}

			if (outputs.length > 0) {
				// don't copy for the last output
				outputs[outputs.length - 1].collect(outputTag, record);
			}
		}
	}

	abstract static class AbstractStreamOperatorProxy<OUT> implements StreamOperator<OUT> {

		private final StreamOperator<OUT> operator;
		protected final List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>> successors;

		AbstractStreamOperatorProxy(
				StreamOperator<OUT> operator,
				List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>> successors) {
			this.operator = operator;
			this.successors = successors;
		}

		public StreamOperator<OUT> getOperator() {
			return operator;
		}

		@SuppressWarnings("unchecked")
		public static AbstractStreamOperatorProxy proxy(StreamOperator operator, List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>> successors) {
			if (operator instanceof OneInputStreamOperator) {
				return new OneInputStreamOperatorProxy((OneInputStreamOperator) operator, successors);
			} else if (operator instanceof TwoInputStreamOperator) {
				return new TwoInputStreamOperatorProxy((TwoInputStreamOperator) operator, successors);
			} else if (operator instanceof StreamSource) {
				return new SourceStreamOperatorProxy((StreamSource) operator, successors);
			} else if (operator instanceof StreamSourceV2) {
				return new SourceV2StreamOperatorProxy(operator, successors);
			} else {
				throw new RuntimeException("Unknown input stream operator " + operator);
			}
		}

		public abstract void addInputEdge(StreamEdge inputEdge);

		public abstract void endInput(StreamEdge inputEdge) throws Exception;

		public boolean isSelected(@Nullable StreamEdge inputEdge, @Nullable Map<StreamEdge, Boolean> visited) {
			if (inputEdge != null && visited != null) {
				final Boolean isSelected = visited.get(inputEdge);
				if (isSelected != null) {
					return isSelected;
				}
			}

			for (Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge> successor : successors) {
				if (!successor.f0.isSelected(successor.f1, visited)) {
					if (inputEdge != null && visited != null) {
						visited.put(inputEdge, false);
					}
					return false;
				}
			}
			if (inputEdge != null && visited != null) {
				visited.put(inputEdge, true);
			}
			return true;
		}

		public void endSuccessorsInput() throws Exception {
			for (Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge> successor : successors) {
				successor.f0.endInput(successor.f1);
			}
		}

		@Override
		public void setCurrentKey(Object key) {
			operator.setCurrentKey(key);
		}

		@Override
		public Object getCurrentKey() {
			return operator.getCurrentKey();
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			operator.notifyCheckpointComplete(checkpointId);
		}

		@Override
		public void setup(
				StreamTask<?, ?> containingTask,
				StreamConfig config,
				Output<StreamRecord<OUT>> output) {
			operator.setup(containingTask, config, output);
		}

		@Override
		public void open() throws Exception {
			operator.open();
		}

		@Override
		public void close() throws Exception {
			operator.close();
		}

		@Override
		public void dispose() throws Exception {
			operator.dispose();
		}

		@Override
		public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
			operator.prepareSnapshotPreBarrier(checkpointId);
		}

		@Override
		public OperatorSnapshotFutures snapshotState(
				long checkpointId,
				long timestamp,
				CheckpointOptions checkpointOptions,
				CheckpointStreamFactory storageLocation) throws Exception {

			return operator.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
		}

		@Override
		public void initializeState() throws Exception {
			operator.initializeState();
		}

		@Override
		public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
			operator.setKeyContextElement1(record);
		}

		@Override
		public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
			operator.setKeyContextElement2(record);
		}

		@Override
		public ChainingStrategy getChainingStrategy() {
			return operator.getChainingStrategy();
		}

		@Override
		public void setChainingStrategy(ChainingStrategy strategy) {
			operator.setChainingStrategy(strategy);
		}

		@Override
		public MetricGroup getMetricGroup() {
			return operator.getMetricGroup();
		}

		@Override
		public OperatorID getOperatorID() {
			return operator.getOperatorID();
		}
	}

	private static class OneInputStreamOperatorProxy<IN, OUT> extends AbstractStreamOperatorProxy<OUT> implements OneInputStreamOperator<IN, OUT> {

		private final OneInputStreamOperator<IN, OUT> operator;
		private volatile int unfinishedInputEdges = 0;

		OneInputStreamOperatorProxy(
				OneInputStreamOperator<IN, OUT> operator,
				List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>> successors) {
			super(operator, successors);
			this.operator = operator;
		}

		public void addInputEdge(StreamEdge inputEdge) {
			unfinishedInputEdges++;
		}

		public void endInput(StreamEdge inputEdge) throws Exception {
			endInput();
		}

		@Override
		public void processElement(StreamRecord<IN> element) throws Exception {
			operator.processElement(element);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			operator.processWatermark(mark);
		}

		@Override
		public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			operator.processLatencyMarker(latencyMarker);
		}

		@Override
		public void endInput() throws Exception {
			if (--unfinishedInputEdges == 0) {
				operator.endInput();
				endSuccessorsInput();
			} else if (unfinishedInputEdges < 0) {
				throw new RuntimeException("This input side is finished already, unexpected endInput invoked");
			}
		}

		@Override
		public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
			operator.prepareSnapshotPreBarrier(checkpointId);
		}

		@Override
		public boolean requireState() {
			return operator.requireState();
		}
	}

	private static class TwoInputStreamOperatorProxy<IN1, IN2, OUT> extends AbstractStreamOperatorProxy<OUT> implements TwoInputStreamOperator<IN1, IN2, OUT> {

		private final TwoInputStreamOperator<IN1, IN2, OUT> operator;

		private volatile int unfinishedInputEdges1 = 0;
		private volatile int unfinishedInputEdges2 = 0;

		private TwoInputSelection lastSelection = null;

		private SelectionChangedListener listener;

		TwoInputStreamOperatorProxy(
				TwoInputStreamOperator<IN1, IN2, OUT> operator,
				List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>> successors) {
			super(operator, successors);
			this.operator = operator;
		}

		@Override
		public TwoInputSelection firstInputSelection() {
			lastSelection = operator.firstInputSelection();
			return lastSelection;
		}

		@Override
		public TwoInputSelection processElement1(StreamRecord<IN1> element) throws Exception {
			final TwoInputSelection selection = operator.processElement1(element);
			if (selection != lastSelection) {
				lastSelection = selection;
				if (listener != null) {
					listener.notifySelectionChanged();
				}
			}
			return lastSelection;
		}

		@Override
		public TwoInputSelection processElement2(StreamRecord<IN2> element) throws Exception {
			final TwoInputSelection selection = operator.processElement2(element);
			if (selection != lastSelection) {
				lastSelection = selection;
				if (listener != null) {
					listener.notifySelectionChanged();
				}
			}
			return lastSelection;
		}

		@Override
		public void processWatermark1(Watermark mark) throws Exception {
			operator.processWatermark1(mark);
		}

		@Override
		public void processWatermark2(Watermark mark) throws Exception {
			operator.processWatermark2(mark);
		}

		@Override
		public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
			operator.processLatencyMarker1(latencyMarker);
		}

		@Override
		public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
			operator.processLatencyMarker2(latencyMarker);
		}

		@Override
		public void endInput1() throws Exception {
			if (--unfinishedInputEdges1 == 0) {
				operator.endInput1();
				if (lastSelection != TwoInputSelection.SECOND) {
					lastSelection = TwoInputSelection.SECOND;
					if (listener != null) {
						listener.notifySelectionChanged();
					}
				}
			} else if (unfinishedInputEdges1 < 0) {
				throw new RuntimeException("This input side is finished already, unexpected endInput invoked");
			}
			if (unfinishedInputEdges1 == 0 && unfinishedInputEdges2 == 0) {
				endSuccessorsInput();
			}
		}

		@Override
		public void endInput2() throws Exception {
			if (--unfinishedInputEdges2 == 0) {
				operator.endInput2();
				if (lastSelection != TwoInputSelection.FIRST) {
					lastSelection = TwoInputSelection.FIRST;
					if (listener != null) {
						listener.notifySelectionChanged();
					}
				}
			} else if (unfinishedInputEdges2 < 0) {
				throw new RuntimeException("This input side is finished already, unexpected endInput invoked");
			}
			if (unfinishedInputEdges1 == 0 && unfinishedInputEdges2 == 0) {
				endSuccessorsInput();
			}
		}

		@Override
		public void addInputEdge(StreamEdge inputEdge) {
			if (inputEdge.getTypeNumber() == 1) {
				unfinishedInputEdges1++;
			} else if (inputEdge.getTypeNumber() == 2) {
				unfinishedInputEdges2++;
			} else {
				throw new RuntimeException("Unknown stream edge type number " + inputEdge.getTypeNumber());
			}
		}

		@Override
		public void endInput(StreamEdge inputEdge) throws Exception {
			if (inputEdge.getTypeNumber() == 1) {
				endInput1();
			} else if (inputEdge.getTypeNumber() == 2) {
				endInput2();
			} else {
				throw new RuntimeException("Unknown stream edge type number " + inputEdge.getTypeNumber());
			}
		}

		@Override
		public boolean isSelected(StreamEdge inputEdge, Map<StreamEdge, Boolean> visited) {
			Preconditions.checkNotNull(inputEdge);

			if (lastSelection == null) {
				// For the first visiting
				lastSelection = firstInputSelection();
			}
			if ((inputEdge.getTypeNumber() == 1 && lastSelection == TwoInputSelection.SECOND) ||
				(inputEdge.getTypeNumber() == 2 && lastSelection == TwoInputSelection.FIRST)) {
				if (visited != null) {
					visited.put(inputEdge, false);
				}
				return false;
			}

			Boolean isSelected;
			if (visited != null) {
				isSelected = visited.get(inputEdge);
				if (isSelected != null) {
					return isSelected;
				}
			}

			isSelected = super.isSelected(inputEdge, visited);
			if (visited != null) {
				visited.put(inputEdge, isSelected);
			}
			return isSelected;
		}

		public void registerSelectionChangedListener(SelectionChangedListener listener) {
			this.listener = listener;
		}

		@Override
		public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
			operator.prepareSnapshotPreBarrier(checkpointId);
		}

		@Override
		public boolean requireState() {
			return operator.requireState();
		}
	}

	private static class SourceStreamOperatorProxy<OUT> extends AbstractStreamOperatorProxy<OUT> implements OneInputStreamOperator<OUT, OUT> {

		private final StreamSource<OUT, ?> operator;

		SourceStreamOperatorProxy(StreamSource<OUT, ?> operator, List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>> successors) {
			super(operator, successors);

			this.operator = operator;
		}

		@Override
		public void addInputEdge(StreamEdge inputEdge) {
			throw new UnsupportedOperationException("There should not be a input edge in source operator");
		}

		@Override
		public void endInput(StreamEdge inputEdge) throws Exception {
			endInput();
		}

		@Override
		public void processElement(StreamRecord<OUT> element) throws Exception {
			operator.getOutput().collect(element);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			operator.getOutput().emitWatermark(mark);
		}

		@Override
		public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			operator.getOutput().emitLatencyMarker(latencyMarker);
		}

		@Override
		public void endInput() throws Exception {
			endSuccessorsInput();
		}

		@Override
		public boolean requireState() {
			return operator.requireState();
		}
	}

	private static class SourceV2StreamOperatorProxy<OUT> extends AbstractStreamOperatorProxy<OUT> implements OneInputStreamOperator<OUT, OUT> {

		SourceV2StreamOperatorProxy(StreamOperator<OUT> operator, List<Tuple2<AbstractStreamOperatorProxy<?>, StreamEdge>> successors) {
			super(operator, successors);
		}

		@Override
		public void addInputEdge(StreamEdge inputEdge) {
			throw new UnsupportedOperationException("There should not be a input edge in source operator");
		}

		@Override
		public void endInput(StreamEdge inputEdge) throws Exception {
			endSuccessorsInput();
		}

		@Override
		public void processElement(StreamRecord<OUT> element) throws Exception {
			throw new UnsupportedOperationException("Should not come to here");
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			throw new UnsupportedOperationException("Should not come to here");
		}

		@Override
		public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			throw new UnsupportedOperationException("Should not come to here");
		}

		@Override
		public void endInput() throws Exception {
			endSuccessorsInput();
		}

		@Override
		public boolean requireState() {
			return getOperator().requireState();
		}
	}

	/**
	 * To get the topology sorted operators, https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm.
	 * @param headIds
	 * @param userCodeClassloader
	 * @param allOperators
	 * @param chainedConfigs
	 * @return topology sorted operators.
	 */
	static Deque<StreamOperator<?>> getTopologySortedOperators(
		List<Integer> headIds,
		ClassLoader userCodeClassloader,
		Map<Integer, ? extends StreamOperator<?>> allOperators,
		Map<Integer, StreamConfig> chainedConfigs) {

		// For iteration mode
		if (allOperators == null || allOperators.isEmpty()) {
			return new ArrayDeque<>();
		}

		final Queue<Integer> toTraversed = new ArrayDeque<>();

		final Map<Integer, Integer> operatorInputs = new HashMap<>();
		for (StreamConfig streamConfig : chainedConfigs.values()) {
			for (StreamEdge edgeInChain : streamConfig.getChainedOutputs(userCodeClassloader)) {
				operatorInputs.put(edgeInChain.getTargetId(), operatorInputs.getOrDefault(edgeInChain.getTargetId(), 0) + 1);
			}
		}

		// Traverse the operators which are without input edges in chain first
		for (int headId : headIds) {
			if (operatorInputs.getOrDefault(headId, 0) == 0) {
				toTraversed.add(headId);
			}
		}

		checkState(!toTraversed.isEmpty());

		final Deque<StreamOperator<?>> topologySortedOperators = new ArrayDeque<>();

		while (!toTraversed.isEmpty()) {
			final int currentOperatorId = toTraversed.poll();
			topologySortedOperators.add(allOperators.get(currentOperatorId));

			for (StreamEdge edge : chainedConfigs.get(currentOperatorId).getChainedOutputs(userCodeClassloader)) {
				final int targetOperatorId = edge.getTargetId();
				int inputCountsLeft = operatorInputs.get(targetOperatorId);
				// Reduce one input edge of target operator
				operatorInputs.put(targetOperatorId, --inputCountsLeft);
				if (inputCountsLeft == 0) {
					toTraversed.add(targetOperatorId);
				}
			}
		}
		return topologySortedOperators;
	}
}
