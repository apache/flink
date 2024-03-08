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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This verifies that slot sharing groups are correctly forwarded from user job to JobGraph.
 *
 * <p>These tests also implicitly verify that chaining does not work across resource groups/slot
 * sharing groups.
 */
@SuppressWarnings("serial")
class SlotAllocationTest {

    private static final FilterFunction<Long> DUMMY_FILTER = value -> false;

    @Test
    void testTwoPipelines() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSequence(1, 10)
                .filter(DUMMY_FILTER)
                .slotSharingGroup("isolated")
                .filter(DUMMY_FILTER)
                .slotSharingGroup("default")
                .disableChaining()
                .filter(DUMMY_FILTER)
                .slotSharingGroup("group 1")
                .filter(DUMMY_FILTER)
                .startNewChain()
                .print()
                .disableChaining();

        // verify that a second pipeline does not inherit the groups from the first pipeline
        env.fromSequence(1, 10)
                .filter(DUMMY_FILTER)
                .slotSharingGroup("isolated-2")
                .filter(DUMMY_FILTER)
                .slotSharingGroup("default")
                .disableChaining()
                .filter(DUMMY_FILTER)
                .slotSharingGroup("group 2")
                .filter(DUMMY_FILTER)
                .startNewChain()
                .print()
                .disableChaining();

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        assertThat(vertices.get(0).getSlotSharingGroup())
                .isEqualTo(vertices.get(3).getSlotSharingGroup())
                .isNotEqualTo(vertices.get(2).getSlotSharingGroup());
        assertThat(vertices.get(3).getSlotSharingGroup())
                .isNotEqualTo(vertices.get(4).getSlotSharingGroup());
        assertThat(vertices.get(4).getSlotSharingGroup())
                .isEqualTo(vertices.get(5).getSlotSharingGroup());
        assertThat(vertices.get(5).getSlotSharingGroup())
                .isEqualTo(vertices.get(6).getSlotSharingGroup());

        int pipelineStart = 6;
        assertThat(vertices.get(1).getSlotSharingGroup())
                .isEqualTo(vertices.get(pipelineStart + 2).getSlotSharingGroup())
                .isNotEqualTo(vertices.get(pipelineStart + 1).getSlotSharingGroup());
        assertThat(vertices.get(pipelineStart + 2).getSlotSharingGroup())
                .isNotEqualTo(vertices.get(pipelineStart + 3).getSlotSharingGroup());
        assertThat(vertices.get(pipelineStart + 3).getSlotSharingGroup())
                .isEqualTo(vertices.get(pipelineStart + 4).getSlotSharingGroup());
        assertThat(vertices.get(pipelineStart + 4).getSlotSharingGroup())
                .isEqualTo(vertices.get(pipelineStart + 5).getSlotSharingGroup());
    }

    @Test
    void testUnion() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> src1 = env.fromSequence(1, 10);
        DataStream<Long> src2 = env.fromSequence(1, 10).slotSharingGroup("src-1");

        // this should not inherit group "src-1"
        src1.union(src2).filter(DUMMY_FILTER);

        DataStream<Long> src3 = env.fromSequence(1, 10).slotSharingGroup("group-1");
        DataStream<Long> src4 = env.fromSequence(1, 10).slotSharingGroup("group-1");

        // this should inherit "group-1" now
        src3.union(src4).filter(DUMMY_FILTER);

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        // first pipeline
        assertThat(vertices.get(0).getSlotSharingGroup())
                .isEqualTo(vertices.get(4).getSlotSharingGroup())
                .isNotEqualTo(vertices.get(1).getSlotSharingGroup());
        assertThat(vertices.get(1).getSlotSharingGroup())
                .isNotEqualTo(vertices.get(4).getSlotSharingGroup());

        // second pipeline
        assertThat(vertices.get(2).getSlotSharingGroup())
                .isEqualTo(vertices.get(3).getSlotSharingGroup())
                .isEqualTo(vertices.get(5).getSlotSharingGroup());
        assertThat(vertices.get(3).getSlotSharingGroup())
                .isEqualTo(vertices.get(5).getSlotSharingGroup());
    }

    @Test
    void testInheritOverride() {
        // verify that we can explicitly disable inheritance of the input slot sharing groups

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> src1 = env.fromSequence(1, 10).slotSharingGroup("group-1");
        DataStream<Long> src2 = env.fromSequence(1, 10).slotSharingGroup("group-1");

        // this should not inherit group but be in "default"
        src1.union(src2).filter(DUMMY_FILTER).slotSharingGroup("default");
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        assertThat(vertices.get(0).getSlotSharingGroup())
                .isEqualTo(vertices.get(1).getSlotSharingGroup())
                .isNotEqualTo(vertices.get(2).getSlotSharingGroup());
        assertThat(vertices.get(1).getSlotSharingGroup())
                .isNotEqualTo(vertices.get(2).getSlotSharingGroup());
    }

    @Test
    void testCoOperation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CoMapFunction<Long, Long, Long> dummyCoMap =
                new CoMapFunction<Long, Long, Long>() {
                    @Override
                    public Long map1(Long value) throws Exception {
                        return null;
                    }

                    @Override
                    public Long map2(Long value) throws Exception {
                        return null;
                    }
                };

        DataStream<Long> src1 = env.fromSequence(1, 10);
        DataStream<Long> src2 = env.fromSequence(1, 10).slotSharingGroup("src-1");

        // this should not inherit group "src-1"
        src1.connect(src2).map(dummyCoMap);

        DataStream<Long> src3 = env.fromSequence(1, 10).slotSharingGroup("group-1");
        DataStream<Long> src4 = env.fromSequence(1, 10).slotSharingGroup("group-1");

        // this should inherit "group-1" now
        src3.connect(src4).map(dummyCoMap);

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        // first pipeline
        assertThat(vertices.get(0).getSlotSharingGroup())
                .isEqualTo(vertices.get(4).getSlotSharingGroup())
                .isNotEqualTo(vertices.get(1).getSlotSharingGroup());
        assertThat(vertices.get(1).getSlotSharingGroup())
                .isNotEqualTo(vertices.get(4).getSlotSharingGroup());

        // second pipeline
        assertThat(vertices.get(2).getSlotSharingGroup())
                .isEqualTo(vertices.get(3).getSlotSharingGroup())
                .isEqualTo(vertices.get(5).getSlotSharingGroup());
        assertThat(vertices.get(3).getSlotSharingGroup())
                .isEqualTo(vertices.get(5).getSlotSharingGroup());
    }
}
