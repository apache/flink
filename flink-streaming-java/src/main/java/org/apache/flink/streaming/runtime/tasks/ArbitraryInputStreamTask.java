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
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.operators.StreamSourceV2;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.InputProcessorUtil;
import org.apache.flink.streaming.runtime.io.SelectedReadingBarrierHandler;
import org.apache.flink.streaming.runtime.io.StreamArbitraryInputProcessor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusSubMaintainer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * A {@link StreamTask} for executing an ArbitraryInputStreamOperator.
 */
@Internal
public class ArbitraryInputStreamTask<OUT> extends StreamTask<OUT, StreamOperator<OUT>> {

	private StreamArbitraryInputProcessor processor;

	public ArbitraryInputStreamTask(Environment env) {
		super(env);
	}

	public ArbitraryInputStreamTask(Environment environment, @Nullable ProcessingTimeService timeProvider) {
		super(environment, timeProvider);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init() throws Exception {

		final StreamTaskConfigSnapshot configuration = getStreamTaskConfig();
		final ClassLoader userClassLoader = getUserCodeClassLoader();

		SelectedReadingBarrierHandler barrierHandler = null;
		if (getEnvironment().getAllInputGates().length > 0) {
			barrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
				configuration.isCheckpointingEnabled(),
				this,
				configuration.getCheckpointMode(),
				getEnvironment().getIOManager(),
				getEnvironment().getTaskManagerInfo().getConfiguration(),
				getEnvironment().getAllInputGates());
		}

		processor = new StreamArbitraryInputProcessor(
			getEnvironment().getIOManager(),
			getCheckpointLock(),
			operatorChain,
			getEnvironment().getMetricGroup(),
			barrierHandler);

		final BitSet subStreamStatus = new BitSet();

		int sourceCount = 0;
		for (int headId : configuration.getChainedHeadNodeIds()) {
			StreamOperator streamOperator = operatorChain.getHeadOperator(headId);

			if (streamOperator instanceof StreamSource) {
				throw new UnsupportedOperationException("Source operator v1 is not supported in ArbitraryInputStreamTask");
			} else if (streamOperator instanceof StreamSourceV2) {
				StreamSourceV2 operator = (StreamSourceV2) streamOperator;

				final StreamStatusSubMaintainer
					streamStatusSubMaintainer = new StreamStatusSubMaintainer(
					operatorChain,
					subStreamStatus,
					sourceCount);

				final TimeCharacteristic timeCharacteristic = operator.getOperatorConfig().getTimeCharacteristic();
				Output<StreamRecord<OUT>> collector = operator.getOutput();
				final long watermarkInterval = operator.getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();

				SourceContext<OUT> context = StreamSourceContexts.getSourceContext(
					timeCharacteristic,
					getProcessingTimeService(),
					getCheckpointLock(),
					streamStatusSubMaintainer,
					collector,
					watermarkInterval,
					-1);

				processor.bindSourceOperator(headId, operator, (OneInputStreamOperator) operatorChain.getOperatorProxy(headId), context, streamStatusSubMaintainer);
			}
		}

		final Map<Integer, StreamConfig> operatorConfigs = configuration.getChainedNodeConfigs();
		int channelCount = 0;

		final List<StreamEdge> inEdges = configuration.getInStreamEdgesOfChain();

		for (int i = 0; i < inEdges.size(); i++) {
			final StreamEdge streamEdge = inEdges.get(i);
			final InputGate inputGate = getEnvironment().getInputGate(i);

			final StreamOperator headOperator = operatorChain.getOperatorProxy(streamEdge.getTargetId());
			Preconditions.checkNotNull(headOperator);

			final StreamConfig operatorConfig = operatorConfigs.get(streamEdge.getTargetId());
			Preconditions.checkNotNull(operatorConfig);

			final StreamStatusSubMaintainer streamStatusSubMaintainer = new StreamStatusSubMaintainer(
				operatorChain,
				subStreamStatus,
				sourceCount + i);

			if (headOperator instanceof OneInputStreamOperator) {
				processor.bindOneInputOperator(
					streamEdge,
					inputGate,
					channelCount,
					(OneInputStreamOperator) headOperator,
					operatorConfig.getTypeSerializerIn1(userClassLoader),
					streamStatusSubMaintainer,
					getExecutionConfig().isObjectReuseEnabled(),
					getEnvironment().getTaskManagerInfo().getConfiguration());
			} else if (headOperator instanceof TwoInputStreamOperator) {
				if (streamEdge.getTypeNumber() == 1) {
					processor.bindFirstOfTwoInputOperator(
						streamEdge,
						inputGate,
						channelCount,
						(TwoInputStreamOperator) headOperator,
						operatorConfig.getTypeSerializerIn1(userClassLoader),
						streamStatusSubMaintainer,
						getExecutionConfig().isObjectReuseEnabled(),
						getEnvironment().getTaskManagerInfo().getConfiguration());
				} else {
					processor.bindSecondOfTwoInputOperator(
						streamEdge,
						inputGate,
						channelCount,
						(TwoInputStreamOperator) headOperator,
						operatorConfig.getTypeSerializerIn2(userClassLoader),
						streamStatusSubMaintainer,
						getExecutionConfig().isObjectReuseEnabled(),
						getEnvironment().getTaskManagerInfo().getConfiguration());
				}
			} else {
				throw new RuntimeException("Unsupported of " + headOperator + " yet");
			}
			channelCount += inputGate.getNumberOfInputChannels();
		}
	}

	@Override
	protected void run() throws Exception {
		processor.process();
	}

	@Override
	protected void cleanup() throws Exception {
		if (processor != null) {
			processor.cleanup();
		}
	}

	@Override
	protected void cancelTask() {
		if (processor != null) {
			processor.stop();
		}
	}
}
