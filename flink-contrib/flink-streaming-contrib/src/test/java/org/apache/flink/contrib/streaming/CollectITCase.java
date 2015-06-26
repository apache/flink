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

package org.apache.flink.contrib.streaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.Test;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Assert;

import java.util.Iterator;

/**
 * This test verifies the behavior of DataStreamUtils.collect.
 */
public class CollectITCase {

	@Test
	public void testCollect() {

		Configuration config = new Configuration();
		ForkableFlinkMiniCluster cluster = new ForkableFlinkMiniCluster(config, false);
		cluster.start();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getLeaderRPCPort());

		long N = 10;
		DataStream<Long> stream = env.generateSequence(1, N);

		long i = 1;
		for(Iterator it = DataStreamUtils.collect(stream); it.hasNext(); ) {
			Long x = (Long) it.next();
			if(x != i) {
				Assert.fail(String.format("Should have got %d, got %d instead.", i, x));
			}
			i++;
		}
		if(i != N + 1) {
			Assert.fail(String.format("Should have collected %d numbers, got %d instead.", N, i - 1));
		}
	}
}
