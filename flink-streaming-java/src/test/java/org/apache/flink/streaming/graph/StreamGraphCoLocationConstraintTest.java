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

package org.apache.flink.streaming.graph;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test that check the hidden API to set co location constraints on the stream transformations. */
class StreamGraphCoLocationConstraintTest {

    @Test
    void testSettingCoLocationConstraint() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(7);

        // set up the test program
        DataStream<Long> source = env.fromSequence(1L, 10_000_000);
        source.getTransformation().setCoLocationGroupKey("group1");

        DataStream<Long> step1 = source.keyBy(v -> v).map(v -> v);
        step1.getTransformation().setCoLocationGroupKey("group2");

        DataStream<Long> step2 = step1.keyBy(v -> v).map(v -> v);
        step2.getTransformation().setCoLocationGroupKey("group1");

        DataStreamSink<Long> result = step2.keyBy(v -> v).sinkTo(new DiscardingSink<>());
        result.getTransformation().setCoLocationGroupKey("group2");

        // get the graph
        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(4);

        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        for (JobVertex vertex : vertices) {
            assertThat(vertex.getCoLocationGroup()).isNotNull();
        }

        assertThat(vertices.get(0).getCoLocationGroup())
                .isEqualTo(vertices.get(2).getCoLocationGroup());
        assertThat(vertices.get(1).getCoLocationGroup())
                .isEqualTo(vertices.get(3).getCoLocationGroup());
    }

    @Test
    void testCoLocateDifferenSharingGroups() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(7);

        // set up the test program
        DataStream<Long> source = env.fromSequence(1L, 10_000_000);
        source.getTransformation().setSlotSharingGroup("ssg1");
        source.getTransformation().setCoLocationGroupKey("co1");

        DataStream<Long> step1 = source.keyBy(v -> v).map(v -> v);
        step1.getTransformation().setSlotSharingGroup("ssg2");
        step1.getTransformation().setCoLocationGroupKey("co2");

        DataStream<Long> step2 = step1.keyBy(v -> v).map(v -> v);
        step2.getTransformation().setSlotSharingGroup("ssg3");
        step2.getTransformation().setCoLocationGroupKey("co1");

        DataStreamSink<Long> result = step2.keyBy(v -> v).sinkTo(new DiscardingSink<>());
        result.getTransformation().setSlotSharingGroup("ssg4");
        result.getTransformation().setCoLocationGroupKey("co2");

        // get the graph
        assertThatThrownBy(() -> env.getStreamGraph().getJobGraph())
                .isInstanceOf(IllegalStateException.class);
    }
}
