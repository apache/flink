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

package org.apache.flink.api.datastream;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@code DataStream} collect methods.
 */
public class DataStreamCollectTestITCase extends TestLogger {

	private static final int PARALLELISM = 4;

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
					.setNumberTaskManagers(1)
					.setNumberSlotsPerTaskManager(PARALLELISM)
					.build());

	@Test
	public void testStreamingCollect() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Integer> stream = env.fromElements(1, 2, 3);

		try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
			List<Integer> results = CollectionUtil.iteratorToList(iterator);
			Assert.assertThat("Failed to collect all data from the stream", results,
					Matchers.containsInAnyOrder(1, 2, 3));
		}
	}

	@Test
	public void testStreamingCollectAndLimit() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Integer> stream = env.fromElements(1, 2, 3, 4, 5);

		List<Integer> results = stream.executeAndCollect(1);
		Assert.assertEquals(
				"Failed to collect the correct number of elements from the stream",
				1,
				results.size());
	}
}
