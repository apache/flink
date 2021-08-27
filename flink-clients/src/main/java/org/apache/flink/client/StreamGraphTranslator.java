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
 *
 */

package org.apache.flink.client;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link FlinkPipelineTranslator} for DataStream API {@link StreamGraph StreamGraphs}.
 *
 * <p>Note: this is used through reflection in {@link
 * org.apache.flink.client.FlinkPipelineTranslationUtil}.
 */
@SuppressWarnings("unused")
public class StreamGraphTranslator implements FlinkPipelineTranslator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamGraphTranslator.class);

    @Override
    public JobGraph translateToJobGraph(
            Pipeline pipeline, Configuration optimizerConfiguration, int defaultParallelism) {
        checkArgument(
                pipeline instanceof StreamGraph, "Given pipeline is not a DataStream StreamGraph.");

        StreamGraph streamGraph = (StreamGraph) pipeline;
        return streamGraph.getJobGraph(null);
    }

    @Override
    public String translateToJSONExecutionPlan(Pipeline pipeline) {
        checkArgument(
                pipeline instanceof StreamGraph, "Given pipeline is not a DataStream StreamGraph.");

        StreamGraph streamGraph = (StreamGraph) pipeline;

        return streamGraph.getStreamingPlanAsJSON();
    }

    @Override
    public boolean canTranslate(Pipeline pipeline) {
        return pipeline instanceof StreamGraph;
    }
}
