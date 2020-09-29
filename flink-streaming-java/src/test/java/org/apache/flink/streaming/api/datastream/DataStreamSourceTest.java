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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link DataStreamSource}.
 */
public class DataStreamSourceTest {

	/**
	 * Test constructor for new Sources (FLIP-27).
	 */
	@Test
	public void testConstructor() {
		int expectParallelism = 100;
		boolean expectIsParallel = true;
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		MockSource mockSource = new MockSource(Boundedness.BOUNDED, 10);
		DataStreamSource<Integer> stream = env.fromSource(
			mockSource,
			WatermarkStrategy.noWatermarks(),
			"TestingSource");
		stream.setParallelism(expectParallelism);

		assertEquals(expectIsParallel, stream.isParallel);

		assertEquals(expectParallelism, stream.getParallelism());
	}
}
