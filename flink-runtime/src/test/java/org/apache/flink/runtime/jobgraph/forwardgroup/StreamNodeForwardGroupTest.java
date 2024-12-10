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

package org.apache.flink.runtime.jobgraph.forwardgroup;

import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link StreamNodeForwardGroup}. */
class StreamNodeForwardGroupTest {
    @Test
    void testStreamNodeForwardGroup() {
        Set<StreamNode> streamNodes = new HashSet<>();

        streamNodes.add(createStreamNode(0, 1, 1));
        streamNodes.add(createStreamNode(1, 1, 1));

        StreamNodeForwardGroup forwardGroup = new StreamNodeForwardGroup(streamNodes);
        assertThat(forwardGroup.getParallelism()).isEqualTo(1);
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(1);
        assertThat(forwardGroup.size()).isEqualTo(2);

        streamNodes.add(createStreamNode(3, 1, 1));

        StreamNodeForwardGroup forwardGroup2 = new StreamNodeForwardGroup(streamNodes);
        assertThat(forwardGroup2.size()).isEqualTo(3);
    }

    @Test
    void testMergeForwardGroup() {
        Map<Integer, StreamNode> streamNodeRetriever = new HashMap<>();
        StreamNodeForwardGroup forwardGroup =
                createForwardGroupAndUpdateStreamNodeRetriever(
                        createStreamNode(0, -1, -1), streamNodeRetriever);

        StreamNodeForwardGroup forwardGroupWithUnDecidedParallelism =
                createForwardGroupAndUpdateStreamNodeRetriever(
                        createStreamNode(1, -1, -1), streamNodeRetriever);

        forwardGroup.mergeForwardGroup(forwardGroupWithUnDecidedParallelism);
        assertThat(forwardGroup.isParallelismDecided()).isFalse();
        assertThat(forwardGroup.isMaxParallelismDecided()).isFalse();

        StreamNodeForwardGroup forwardGroupWithDecidedParallelism =
                createForwardGroupAndUpdateStreamNodeRetriever(
                        createStreamNode(2, 2, 4), streamNodeRetriever);
        forwardGroup.mergeForwardGroup(forwardGroupWithDecidedParallelism);
        assertThat(forwardGroup.getParallelism()).isEqualTo(2);
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(4);

        StreamNodeForwardGroup forwardGroupWithLargerMaxParallelism =
                createForwardGroupAndUpdateStreamNodeRetriever(
                        createStreamNode(3, 2, 5), streamNodeRetriever);
        // The target max parallelism is larger than source.
        assertThat(forwardGroup.mergeForwardGroup(forwardGroupWithLargerMaxParallelism)).isTrue();
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(4);

        StreamNodeForwardGroup forwardGroupWithSmallerMaxParallelism =
                createForwardGroupAndUpdateStreamNodeRetriever(
                        createStreamNode(4, 2, 3), streamNodeRetriever);
        assertThat(forwardGroup.mergeForwardGroup(forwardGroupWithSmallerMaxParallelism)).isTrue();
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(3);

        StreamNodeForwardGroup forwardGroupWithMaxParallelismSmallerThanSourceParallelism =
                createForwardGroupAndUpdateStreamNodeRetriever(
                        createStreamNode(5, -1, 1), streamNodeRetriever);
        assertThat(
                        forwardGroup.mergeForwardGroup(
                                forwardGroupWithMaxParallelismSmallerThanSourceParallelism))
                .isFalse();

        StreamNodeForwardGroup forwardGroupWithDifferentParallelism =
                createForwardGroupAndUpdateStreamNodeRetriever(
                        createStreamNode(6, 1, 3), streamNodeRetriever);
        assertThat(forwardGroup.mergeForwardGroup(forwardGroupWithDifferentParallelism)).isFalse();

        StreamNodeForwardGroup forwardGroupWithUndefinedParallelism =
                createForwardGroupAndUpdateStreamNodeRetriever(
                        createStreamNode(7, -1, 2), streamNodeRetriever);
        assertThat(forwardGroup.mergeForwardGroup(forwardGroupWithUndefinedParallelism)).isTrue();
        assertThat(forwardGroup.size()).isEqualTo(6);
        assertThat(forwardGroup.getParallelism()).isEqualTo(2);
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(2);

        for (Integer nodeId : forwardGroup.getVertexIds()) {
            StreamNode node = streamNodeRetriever.get(nodeId);
            assertThat(node.getParallelism()).isEqualTo(forwardGroup.getParallelism());
            assertThat(node.getMaxParallelism()).isEqualTo(forwardGroup.getMaxParallelism());
        }
    }

    private static StreamNode createStreamNode(int id, int parallelism, int maxParallelism) {
        StreamNode streamNode =
                new StreamNode(id, null, null, (StreamOperator<?>) null, null, null);
        if (parallelism > 0) {
            streamNode.setParallelism(parallelism);
        }
        if (maxParallelism > 0) {
            streamNode.setMaxParallelism(maxParallelism);
        }
        return streamNode;
    }

    private StreamNodeForwardGroup createForwardGroupAndUpdateStreamNodeRetriever(
            StreamNode streamNode, Map<Integer, StreamNode> streamNodeRetriever) {
        streamNodeRetriever.put(streamNode.getId(), streamNode);
        return new StreamNodeForwardGroup(Collections.singleton(streamNode));
    }
}
