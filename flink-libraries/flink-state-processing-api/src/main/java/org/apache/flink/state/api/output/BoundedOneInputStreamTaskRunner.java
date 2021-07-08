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
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.api.functions.Timestamper;
import org.apache.flink.state.api.runtime.SavepointEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.util.Collector;

/**
 * A {@link RichMapPartitionFunction} that serves as the runtime for a {@link BoundedStreamTask}.
 *
 * <p>The task is executed processing the data in a particular partition instead of the pulling from
 * the network stack. After all data has been processed the runner will output the {@link
 * OperatorSubtaskState} from the snapshot of the bounded task.
 *
 * @param <IN> Type of the input to the partition
 */
@Internal
public class BoundedOneInputStreamTaskRunner<IN>
        extends RichMapPartitionFunction<IN, TaggedOperatorSubtaskState> {

    private static final long serialVersionUID = 1L;

    private final StreamConfig streamConfig;

    private final int maxParallelism;

    private final Timestamper<IN> timestamper;

    private transient SavepointEnvironment env;

    /**
     * Create a new {@link BoundedOneInputStreamTaskRunner}.
     *
     * @param streamConfig The internal configuration for the task.
     * @param maxParallelism The max parallelism of the operator.
     */
    public BoundedOneInputStreamTaskRunner(
            StreamConfig streamConfig, int maxParallelism, Timestamper<IN> timestamper) {

        this.streamConfig = streamConfig;
        this.maxParallelism = maxParallelism;
        this.timestamper = timestamper;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        env =
                new SavepointEnvironment.Builder(getRuntimeContext(), maxParallelism)
                        .setConfiguration(streamConfig.getConfiguration())
                        .build();
    }

    @Override
    public void mapPartition(Iterable<IN> values, Collector<TaggedOperatorSubtaskState> out)
            throws Exception {
        new BoundedStreamTask<>(env, values, timestamper, out).invoke();
    }
}
