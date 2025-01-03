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

package org.apache.flink.streaming.api.graph.util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ImmutableStreamGraph}. */
class ImmutableStreamGraphTest {
    @Test
    void testImmutableStreamGraphGraphContent() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(1L, 3L).map(value -> value).print().setParallelism(env.getParallelism());
        StreamGraph streamGraph = env.getStreamGraph();
        ImmutableStreamGraph immutableStreamGraph =
                new ImmutableStreamGraph(
                        streamGraph, Thread.currentThread().getContextClassLoader());

        for (StreamNode streamNode : streamGraph.getStreamNodes()) {
            isStreamNodeEquals(streamNode, immutableStreamGraph.getStreamNode(streamNode.getId()));
        }
    }

    void isStreamNodeEquals(StreamNode streamNode, ImmutableStreamNode immutableStreamNode) {
        assertThat(immutableStreamNode).isNotNull();
        assertThat(streamNode.getId()).isEqualTo(immutableStreamNode.getId());
        assertThat(streamNode.getInEdges().size())
                .isEqualTo(immutableStreamNode.getInEdges().size());
        assertThat(streamNode.getOutEdges().size())
                .isEqualTo(immutableStreamNode.getOutEdges().size());
        List<StreamEdge> inEdges = streamNode.getInEdges();
        List<ImmutableStreamEdge> immutableInEdges = immutableStreamNode.getInEdges();
        for (int i = 0; i < inEdges.size(); i++) {
            isStreamEdgeEquals(inEdges.get(i), immutableInEdges.get(i));
        }
        List<StreamEdge> outEdges = streamNode.getOutEdges();
        List<ImmutableStreamEdge> immutableOutEdges = immutableStreamNode.getOutEdges();
        for (int i = 0; i < outEdges.size(); i++) {
            isStreamEdgeEquals(outEdges.get(i), immutableOutEdges.get(i));
        }
    }

    void isStreamEdgeEquals(StreamEdge streamEdge, ImmutableStreamEdge immutableStreamEdge) {
        assertThat(immutableStreamEdge).isNotNull();
        assertThat(streamEdge.getEdgeId()).isEqualTo(immutableStreamEdge.getEdgeId());
        assertThat(streamEdge.getSourceId()).isEqualTo(immutableStreamEdge.getSourceId());
        assertThat(streamEdge.getTargetId()).isEqualTo(immutableStreamEdge.getTargetId());
        assertThat(streamEdge.getTypeNumber()).isEqualTo(immutableStreamEdge.getTypeNumber());
    }
}
