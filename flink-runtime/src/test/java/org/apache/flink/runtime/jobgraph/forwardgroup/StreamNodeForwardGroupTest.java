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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link StreamNodeForwardGroup}. */
class StreamNodeForwardGroupTest {
    @Test
    void testStreamNodeForwardGroup() {
        Set<StreamNode> startNodes = new HashSet<>();
        Map<StreamNode, List<StreamNode>> chainedNodes = new HashMap<>();
        StreamNode streamNode1 = createStreamNode(0, 1, 1);
        StreamNode streamNode2 = createStreamNode(1, 1, 1);
        startNodes.add(streamNode1);
        startNodes.add(streamNode2);
        chainedNodes.put(streamNode1, Collections.singletonList(streamNode1));
        chainedNodes.put(streamNode2, Collections.singletonList(streamNode2));

        StreamNodeForwardGroup forwardGroup = new StreamNodeForwardGroup(startNodes, chainedNodes);
        assertThat(forwardGroup.getParallelism()).isEqualTo(1);
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(1);
        assertThat(forwardGroup.size()).isEqualTo(2);

        Set<StreamNode> startNodesWithDifferentMaxParallelism = new HashSet<>();
        StreamNode streamNode3 = createStreamNode(2, 1, 2);
        StreamNode streamNode4 = createStreamNode(3, 1, 4);
        startNodesWithDifferentMaxParallelism.add(streamNode3);
        startNodesWithDifferentMaxParallelism.add(streamNode4);
        chainedNodes.put(streamNode3, Collections.singletonList(streamNode3));
        chainedNodes.put(streamNode4, Collections.singletonList(streamNode4));

        StreamNodeForwardGroup forwardGroup2 =
                new StreamNodeForwardGroup(startNodesWithDifferentMaxParallelism, chainedNodes);
        assertThat(forwardGroup2.getParallelism()).isEqualTo(1);
        assertThat(forwardGroup2.getMaxParallelism()).isEqualTo(2);
        assertThat(forwardGroup2.size()).isEqualTo(2);

        forwardGroup.mergeForwardGroup(forwardGroup2);
        assertThat(forwardGroup.size()).isEqualTo(4);
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(1);
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
}
