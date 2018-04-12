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

package org.apache.flink.test.streaming.experimental;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * This test verifies the behavior of DataStreamUtils.collect().
 *
 * <p>This experimental class is relocated from flink-streaming-contrib. Please see package-info.java
 * for more information.
 */
public class CollectITCase extends AbstractTestBase {

	@Test
	public void testCollect() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		final long n = 10;
		DataStream<Long> stream = env.generateSequence(1, n);

		long i = 1;
		for (Iterator<Long> it = DataStreamUtils.collect(stream); it.hasNext(); ) {
			long x = it.next();
			assertEquals("received wrong element", i, x);
			i++;
		}

		assertEquals("received wrong number of elements", n + 1, i);
	}
}
