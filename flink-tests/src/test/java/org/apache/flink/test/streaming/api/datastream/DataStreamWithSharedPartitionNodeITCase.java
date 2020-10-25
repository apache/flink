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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * This test verifies the data could be partitioned correctly
 * if multiple consumers are connected to the same partitioner
 * node.
 */
public class DataStreamWithSharedPartitionNodeITCase {

	@ClassRule
	public static MiniClusterWithClientResource flinkCluster =
		new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setNumberSlotsPerTaskManager(3)
				.setNumberTaskManagers(1)
				.build());

	@Test
	public void testJobWithSharePartitionNode() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<Integer> source = env
			.fromElements(1, 2, 3, 4)
			.partitionCustom(new TestPartitioner(), f -> f);
		source.addSink(new CollectSink("first"));
		source.addSink(new CollectSink("second")).setParallelism(2);

		env.execute();

		checkSinkResult("first-0", Arrays.asList(1, 2, 3, 4));
		checkSinkResult("second-0", Arrays.asList(1, 3));
		checkSinkResult("second-1", Arrays.asList(2, 4));
	}

	private void checkSinkResult(String nameAndIndex, List<Integer> expected) {
		List<Integer> actualResult = CollectSink.result.get(nameAndIndex);
		assertEquals(expected, actualResult);
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
				String key = name + "-" + getRuntimeContext().getIndexOfThisSubtask();
				result.compute(key, (k, v) -> v == null ? new ArrayList<>() : v)
					.add(value);
			}
		}
	}
}
