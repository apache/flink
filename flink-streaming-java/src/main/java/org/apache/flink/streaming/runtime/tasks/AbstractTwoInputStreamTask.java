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
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Abstract class for executing a {@link TwoInputStreamOperator}. */
@Internal
public abstract class AbstractTwoInputStreamTask<IN1, IN2, OUT>
        extends StreamTask<OUT, TwoInputStreamOperator<IN1, IN2, OUT>> {

    protected final WatermarkGauge input1WatermarkGauge;
    protected final WatermarkGauge input2WatermarkGauge;
    protected final MinWatermarkGauge minInputWatermarkGauge;

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    public AbstractTwoInputStreamTask(Environment env) throws Exception {
        super(env);

        input1WatermarkGauge = new WatermarkGauge();
        input2WatermarkGauge = new WatermarkGauge();
        minInputWatermarkGauge = new MinWatermarkGauge(input1WatermarkGauge, input2WatermarkGauge);
    }

    @Override
    public void init() throws Exception {
        StreamConfig configuration = getConfiguration();
        ClassLoader userClassLoader = getUserCodeClassLoader();

        int numberOfInputs = configuration.getNumberOfNetworkInputs();

        ArrayList<IndexedInputGate> inputList1 = new ArrayList<>();
        ArrayList<IndexedInputGate> inputList2 = new ArrayList<>();

        List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

        for (int i = 0; i < numberOfInputs; i++) {
            int inputType = inEdges.get(i).getTypeNumber();
            IndexedInputGate reader = getEnvironment().getInputGate(i);
            switch (inputType) {
                case 1:
                    inputList1.add(reader);
                    break;
                case 2:
                    inputList2.add(reader);
                    break;
                default:
                    throw new RuntimeException("Invalid input type number: " + inputType);
            }
        }

        createInputProcessor(
                inputList1, inputList2, gateIndex -> inEdges.get(gateIndex).getPartitioner());

        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);
        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_1_WATERMARK, input1WatermarkGauge);
        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_2_WATERMARK, input2WatermarkGauge);
        // wrap watermark gauge since registered metrics must be unique
        getEnvironment()
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge::getValue);
    }

    protected abstract void createInputProcessor(
            List<IndexedInputGate> inputGates1,
            List<IndexedInputGate> inputGates2,
            Function<Integer, StreamPartitioner<?>> gatePartitioners)
            throws Exception;
}
