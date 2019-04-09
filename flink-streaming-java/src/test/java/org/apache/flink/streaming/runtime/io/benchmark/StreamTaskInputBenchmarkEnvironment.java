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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.IterableInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTwoInputSelectableProcessor;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.ExceptionUtils.suppressExceptions;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Context for task-input benchmarks executed by the external
 * <a href="https://github.com/dataArtisans/flink-benchmarks">flink-benchmarks</a> project.
 */
public class StreamTaskInputBenchmarkEnvironment {

	protected final JobID jobId = new JobID();

	private final Object lock = new Object();

	private IOManager ioManager;

	private StreamStatusMaintainer streamStatusMaintainer;

	private TaskIOMetricGroup taskIOMetricGroup;

	public void setUp() throws Exception {
		this.ioManager = new IOManagerAsync();
		this.streamStatusMaintainer = new NoOpStreamStatusMaintainer();
		this.taskIOMetricGroup = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup();
	}

	public void tearDown() {
		suppressExceptions(ioManager::shutdown);
	}

	public ProcessorAndChannels<StreamTwoInputProcessor<?, ?>, IterableInputChannel> createTwoInputProcessor(
		int numInputGates1,
		int numInputGates2,
		int numChannels1PerGate,
		int numChannels2PerGate,
		long numRecords1PerChannel,
		long numRecords2PerChannel,
		long inputValue1,
		long inputValue2,
		TwoInputStreamOperator<Long, Long, ?> streamOperator) throws IOException {

		TypeSerializer<Long> serializer = BasicTypeInfo.LONG_TYPE_INFO.createSerializer(new ExecutionConfig());

		List<IterableInputChannel> inputChannels = new ArrayList<>();

		StreamTwoInputProcessor<Long, Long> inputProcessor = new StreamTwoInputProcessor<>(
			createInputGates(jobId, taskIOMetricGroup,
				numInputGates1, numChannels1PerGate, numRecords1PerChannel, serializer, inputValue1, inputChannels),
			createInputGates(jobId, taskIOMetricGroup,
				numInputGates2, numChannels2PerGate, numRecords2PerChannel, serializer, inputValue2, inputChannels),
			serializer,
			serializer,
			null,
			CheckpointingMode.AT_LEAST_ONCE,
			lock,
			ioManager,
			new Configuration(),
			streamStatusMaintainer,
			streamOperator,
			taskIOMetricGroup,
			new WatermarkGauge(),
			new WatermarkGauge());

		return new ProcessorAndChannels<>(inputProcessor, inputChannels);
	}

	public ProcessorAndChannels<StreamTwoInputSelectableProcessor<?, ?>, IterableInputChannel> createTwoInputSelectableProcessor(
			int numInputGates1,
			int numInputGates2,
			int numChannels1PerGate,
			int numChannels2PerGate,
			long numRecords1PerChannel,
			long numRecords2PerChannel,
			long inputValue1,
			long inputValue2,
			TwoInputStreamOperator<Long, Long, ?> streamOperator) throws IOException {

		TypeSerializer<Long> serializer = BasicTypeInfo.LONG_TYPE_INFO.createSerializer(new ExecutionConfig());

		List<IterableInputChannel> inputChannels = new ArrayList<>();

		StreamTwoInputSelectableProcessor<Long, Long> inputProcessor = new StreamTwoInputSelectableProcessor<>(
			createInputGates(jobId, taskIOMetricGroup,
				numInputGates1, numChannels1PerGate, numRecords1PerChannel, serializer, inputValue1, inputChannels),
			createInputGates(jobId, taskIOMetricGroup,
				numInputGates2, numChannels2PerGate, numRecords2PerChannel, serializer, inputValue2, inputChannels),
			serializer,
			serializer,
			lock,
			ioManager,
			streamStatusMaintainer,
			streamOperator,
			new WatermarkGauge(),
			new WatermarkGauge());

		inputProcessor.init();

		return new ProcessorAndChannels<>(inputProcessor, inputChannels);
	}

	private static <V> Collection<InputGate> createInputGates(
		JobID jobId,
		TaskIOMetricGroup taskIOMetricGroup,
		int numInputGates,
		int numChannelsPerGate,
		long numRecordsPerChannel,
		TypeSerializer<V> valueSerializer,
		V inputValue,
		final List<IterableInputChannel> createdChannels) throws IOException {

		TypeSerializer<StreamElement> outRecordSerializer =	new StreamElementSerializer<>(valueSerializer);
		SerializationDelegate<StreamElement> serializationDelegate = new SerializationDelegate<>(outRecordSerializer);
		serializationDelegate.setInstance(new StreamRecord<>(inputValue));

		List<InputGate> inputGates = new ArrayList<>();
		for (int i = 0; i < numInputGates; i++) {
			inputGates.add(
				createInputGate(
					jobId,
					taskIOMetricGroup,
					numChannelsPerGate,
					numRecordsPerChannel,
					serializationDelegate,
					createdChannels));
		}

		return inputGates;
	}

	private static <T extends IOReadableWritable> InputGate createInputGate(
		JobID jobId,
		TaskIOMetricGroup taskIOMetricGroup,
		int numChannels,
		long numRecordsPerChannel,
		T inputRecord,
		final List<IterableInputChannel> createdChannels) throws IOException {

		SingleInputGate inputGate = new SingleInputGate(
			"benchmark task",
			jobId,
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED_BOUNDED,
			0,
			numChannels,
			new NoOpTaskActions(),
			taskIOMetricGroup,
			true);

		for (int channel = 0; channel < numChannels; ++channel) {
			List<Buffer> dataBuffers = BufferUtil.createNetworkBuffers(numRecordsPerChannel, inputRecord);

			IterableInputChannel inputChannel = new IterableInputChannel(inputGate, channel, dataBuffers);
			inputGate.setInputChannel(new IntermediateResultPartitionID(), inputChannel);

			createdChannels.add(inputChannel);
		}

		return inputGate;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	static class ProcessorAndChannels<P, C> {

		private final P processor;

		private final List<C> channels;

		public ProcessorAndChannels(P processor, List<C> channels) {
			this.processor = checkNotNull(processor);
			this.channels = checkNotNull(channels);
		}

		public P processor() {
			return processor;
		}

		public List<C> channels() {
			return channels;
		}
	}

	// ------------------------------------------------------------------------
	//  Mocks
	// ------------------------------------------------------------------------

	/**
	 * A dummy implementation of the {@link TaskActions}. We implement this here rather than using Mockito
	 * to avoid using mockito in this benchmark class.
	 */
	private static final class NoOpTaskActions implements TaskActions {

		@Override
		public void triggerPartitionProducerStateCheck(
			JobID jobId,
			IntermediateDataSetID intermediateDataSetId,
			ResultPartitionID resultPartitionId) {}

		@Override
		public void failExternally(Throwable cause) {}
	}

	private static final class NoOpStreamStatusMaintainer implements StreamStatusMaintainer {

		@Override
		public StreamStatus getStreamStatus() {
			return StreamStatus.ACTIVE;
		}

		@Override
		public void toggleStreamStatus(StreamStatus streamStatus) {

		}
	}
}
