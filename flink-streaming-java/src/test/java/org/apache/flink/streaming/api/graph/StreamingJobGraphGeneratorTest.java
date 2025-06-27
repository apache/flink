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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterables;

/** Tests for {@link StreamingJobGraphGenerator}. */
@SuppressWarnings("serial")
class StreamingJobGraphGeneratorTest extends JobGraphGeneratorTestBase {
    @Override
    JobGraph createJobGraph(StreamGraph streamGraph) {
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }

    @Override
    void verifyManagedMemoryFractionForUnknownResourceSpec(
            JobVertex vertex1,
            JobVertex vertex2,
            JobVertex vertex3,
            Configuration taskManagerConfig) {
        final StreamConfig sourceConfig = new StreamConfig(vertex1.getConfiguration());
        verifyFractions(sourceConfig, 0.6 / 2, 0.0, 0.0, taskManagerConfig);

        final StreamConfig map1Config =
                Iterables.getOnlyElement(
                        sourceConfig
                                .getTransitiveChainedTaskConfigs(
                                        JobGraphGeneratorTestBase.class.getClassLoader())
                                .values());
        verifyFractions(map1Config, 0.6 / 2, 0.4, 0.0, taskManagerConfig);

        final StreamConfig map2Config = new StreamConfig(vertex2.getConfiguration());
        verifyFractions(map2Config, 0.0, 0.4, 0.0, taskManagerConfig);

        final StreamConfig map3Config = new StreamConfig(vertex3.getConfiguration());
        verifyFractions(map3Config, 1.0, 0.0, 0.0, taskManagerConfig);
    }
}
