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

package org.apache.flink.api.connector.source.lib;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamCollector;

import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests {@link NumberSequenceSource}.
 */
public class NumberSequenceSourceITCase {
	@Rule
	public StreamCollector collector = new StreamCollector();

	@Test
	public void testCheckpointingWithDelayedAssignment() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		final SingleOutputStreamOperator<Long> stream = env
			.fromSequence(0, 100)
			.map(x -> {
				Thread.sleep(10);
				return x;
			});
		final CompletableFuture<Collection<Long>> result = collector.collect(stream);
		env.execute();

		assertArrayEquals(LongStream.rangeClosed(0, 100).boxed().toArray(), result.get().toArray());
	}
}
