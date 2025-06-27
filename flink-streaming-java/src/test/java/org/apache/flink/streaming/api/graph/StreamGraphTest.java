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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Tests for {@link StreamGraph}. */
class StreamGraphTest {
    @Test
    void testTopologicalSort() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //              -> map1-
        //            /         \
        //  (source) -           +-> map2 -> sink
        //            \         /
        //              -------
        final DataStream<Long> source1 = env.fromSequence(0, 3).name("source");
        DataStream<Long> map1 = source1.map((MapFunction<Long, Long>) value -> value).name("map1");
        source1.union(map1)
                .map((MapFunction<Long, Long>) value -> value)
                .name("map2")
                .sinkTo(new DiscardingSink<>())
                .name("sink");
        StreamGraph streamGraph = env.getStreamGraph();
        List<StreamNode> sortedStreamNodes =
                streamGraph.getStreamNodesSortedTopologicallyFromSources();
        assertBefore("source", "map1", sortedStreamNodes);
        assertBefore("source", "map2", sortedStreamNodes);
        assertBefore("map1", "map2", sortedStreamNodes);
        assertBefore("map2", "sink", sortedStreamNodes);
    }

    private static void assertBefore(String node1, String node2, List<StreamNode> streamNodes) {
        StreamNode n1 = checkNotNull(getStreamNodeByName(node1, streamNodes));
        StreamNode n2 = checkNotNull(getStreamNodeByName(node2, streamNodes));
        boolean seenFirst = false;
        for (StreamNode n : streamNodes) {
            if (n == n1) {
                seenFirst = true;
            } else if (n == n2) {
                if (!seenFirst) {
                    Assertions.fail(
                            "The first vertex ("
                                    + n1
                                    + ") is not before the second vertex ("
                                    + n2
                                    + ")");
                }
                break;
            }
        }
    }

    private static StreamNode getStreamNodeByName(String nodeName, List<StreamNode> streamNodes) {
        for (StreamNode streamNode : streamNodes) {
            if (streamNode.getOperatorName().contains(nodeName)) {
                return streamNode;
            }
        }
        return null;
    }
}
