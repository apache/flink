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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test verifies the data could be partitioned correctly if multiple consumers are connected to
 * the same partitioner node.
 */
class DataStreamWithSharedPartitionNodeITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(3)
                            .build());

    @Test
    void testJobWithSharePartitionNode() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Integer> source =
                env.fromData(1, 2, 3, 4).partitionCustom(new TestPartitioner(), f -> f);
        source.addSink(new CollectSink("first"));
        source.addSink(new CollectSink("second")).setParallelism(2);

        env.execute();

        checkSinkResult("first-0", Arrays.asList(1, 2, 3, 4));
        checkSinkResult("second-0", Arrays.asList(1, 3));
        checkSinkResult("second-1", Arrays.asList(2, 4));
    }

    private void checkSinkResult(String nameAndIndex, List<Integer> expected) {
        List<Integer> actualResult = CollectSink.result.get(nameAndIndex);
        assertThat(actualResult).isEqualTo(expected);
    }

    private static class TestPartitioner implements Partitioner<Integer> {
        private int nextChannelToSendTo = -1;

        @Override
        public int partition(Integer key, int numPartitions) {
            nextChannelToSendTo = (nextChannelToSendTo + 1) % numPartitions;
            return nextChannelToSendTo;
        }
    }

    private static class CollectSink extends RichSinkFunction<Integer> {
        private static final Object resultLock = new Object();

        @GuardedBy("resultLock")
        private static final Map<String, List<Integer>> result = new HashMap<>();

        private final String name;

        public CollectSink(String name) {
            this.name = name;
        }

        @Override
        public void invoke(Integer value, Context context) throws Exception {
            synchronized (resultLock) {
                String key = name + "-" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
                result.compute(key, (k, v) -> v == null ? new ArrayList<>() : v).add(value);
            }
        }
    }
}
