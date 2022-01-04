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

package org.apache.flink.state.api.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.api.runtime.SavepointEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * The runtime for a {@link BootstrapStreamTask}.
 *
 * <p>The task is executed processing the data in a particular partition instead of the pulling from
 * the network stack. After all data has been processed the runner will output the {@link
 * OperatorSubtaskState} from the snapshot of the bounded task.
 *
 * @param <IN> Type of the input to the partition
 */
@Internal
@SuppressWarnings({"rawtypes", "unchecked"})
public class BootstrapStreamTaskRunner<IN>
        extends AbstractStreamOperator<TaggedOperatorSubtaskState>
        implements OneInputStreamOperator<IN, TaggedOperatorSubtaskState>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final StreamConfig streamConfig;

    private final int maxParallelism;

    private transient Thread task;

    private transient BlockingQueue<StreamElement> input;

    /**
     * Create a new {@link BootstrapStreamTaskRunner}.
     *
     * @param streamConfig The internal configuration for the task.
     * @param maxParallelism The max parallelism of the operator.
     */
    public BootstrapStreamTaskRunner(StreamConfig streamConfig, int maxParallelism) {
        this.streamConfig = streamConfig;
        this.maxParallelism = maxParallelism;
    }

    @Override
    public void open() throws Exception {
        this.input = new ArrayBlockingQueue<>(16);

        SavepointEnvironment env =
                new SavepointEnvironment.Builder(getRuntimeContext(), maxParallelism)
                        .setConfiguration(streamConfig.getConfiguration())
                        .build();

        this.task =
                new Thread(
                        () -> {
                            BootstrapStreamTask boundedStreamTask;
                            try {
                                boundedStreamTask = new BootstrapStreamTask(env, input, output);
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to construct bootstrap stream task", e);
                            }

                            Throwable error = null;
                            try {
                                boundedStreamTask.invoke();
                            } catch (Exception e) {
                                error = e;
                            }

                            try {
                                boundedStreamTask.cleanUp(error);
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to cleanup task", e);
                            }
                        });
        this.task.setName(
                streamConfig.getOperatorName()
                        + "-bootstrap-thread-"
                        + getRuntimeContext().getIndexOfThisSubtask());
        this.task.start();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        input.put(element);
    }

    @Override
    public void endInput() throws Exception {
        input.put(EndOfDataMarker.INSTANCE);
        task.join();
    }
}
