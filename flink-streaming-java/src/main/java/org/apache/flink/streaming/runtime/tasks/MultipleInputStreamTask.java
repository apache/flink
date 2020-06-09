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
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.runtime.io.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.InputProcessorUtil;
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler;
import org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StreamTask} for executing a {@link MultipleInputStreamOperator} and supporting
 * the {@link MultipleInputStreamOperator} to select input for reading.
 */
@Internal
public class MultipleInputStreamTask<OUT> extends StreamTask<OUT, MultipleInputStreamOperator<OUT>> {
	public MultipleInputStreamTask(Environment env) throws Exception {
		super(env);
	}

	@Override
	public void init() throws Exception {
		StreamConfig configuration = getConfiguration();
		ClassLoader userClassLoader = getUserCodeClassLoader();

		TypeSerializer<?>[] inputDeserializers = configuration.getTypeSerializersIn(userClassLoader);

		ArrayList<IndexedInputGate>[] inputLists = new ArrayList[inputDeserializers.length];
		WatermarkGauge[] watermarkGauges = new WatermarkGauge[inputDeserializers.length];

		for (int i = 0; i < inputDeserializers.length; i++) {
			inputLists[i] = new ArrayList<>();
			watermarkGauges[i] = new WatermarkGauge();
			headOperator.getMetricGroup().gauge(MetricNames.currentInputWatermarkName(i + 1), watermarkGauges[i]);
		}

		MinWatermarkGauge minInputWatermarkGauge = new MinWatermarkGauge(watermarkGauges);
		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);

		List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);
		int numberOfInputs = configuration.getNumberOfInputs();

		for (int i = 0; i < numberOfInputs; i++) {
			int inputType = inEdges.get(i).getTypeNumber();
			IndexedInputGate reader = getEnvironment().getInputGate(i);
			inputLists[inputType - 1].add(reader);
		}

		createInputProcessor(inputLists, inputDeserializers, watermarkGauges);

		// wrap watermark gauge since registered metrics must be unique
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge::getValue);
	}

	protected void createInputProcessor(
			Collection<IndexedInputGate>[] inputGates,
			TypeSerializer<?>[] inputDeserializers,
			WatermarkGauge[] inputWatermarkGauges) {
		MultipleInputSelectionHandler selectionHandler = new MultipleInputSelectionHandler(
			headOperator instanceof InputSelectable ? (InputSelectable) headOperator : null,
			inputGates.length);

		CheckpointedInputGate[] checkpointedInputGates = InputProcessorUtil.createCheckpointedMultipleInputGate(
			this,
			getConfiguration(),
			getChannelStateWriter(),
			getEnvironment().getMetricGroup().getIOMetricGroup(),
			getTaskNameWithSubtaskAndId(),
			inputGates);
		checkState(checkpointedInputGates.length == inputGates.length);

		inputProcessor = new StreamMultipleInputProcessor(
			checkpointedInputGates,
			inputDeserializers,
			getEnvironment().getIOManager(),
			getStreamStatusMaintainer(),
			headOperator,
			selectionHandler,
			inputWatermarkGauges,
			operatorChain,
			setupNumRecordsInCounter(headOperator));
	}
}
