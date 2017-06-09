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

package org.apache.flink.streaming.graph;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test translation of {@link CheckpointingMode}.
 */
@SuppressWarnings("serial")
public class TranslationTest {

	@Test
	public void testCheckpointModeTranslation() {
		try {
			// with deactivated fault tolerance, the checkpoint mode should be at-least-once
			StreamExecutionEnvironment deactivated = getSimpleJob();

			for (JobVertex vertex : deactivated.getStreamGraph().getJobGraph().getVertices()) {
				assertEquals(CheckpointingMode.AT_LEAST_ONCE, new StreamConfig(vertex.getConfiguration()).getCheckpointMode());
			}

			// with activated fault tolerance, the checkpoint mode should be by default exactly once
			StreamExecutionEnvironment activated = getSimpleJob();
			activated.enableCheckpointing(1000L);
			for (JobVertex vertex : activated.getStreamGraph().getJobGraph().getVertices()) {
				assertEquals(CheckpointingMode.EXACTLY_ONCE, new StreamConfig(vertex.getConfiguration()).getCheckpointMode());
			}

			// explicitly setting the mode
			StreamExecutionEnvironment explicit = getSimpleJob();
			explicit.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE);
			for (JobVertex vertex : explicit.getStreamGraph().getJobGraph().getVertices()) {
				assertEquals(CheckpointingMode.AT_LEAST_ONCE, new StreamConfig(vertex.getConfiguration()).getCheckpointMode());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static StreamExecutionEnvironment getSimpleJob() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.generateSequence(1, 10000000)
				.addSink(new SinkFunction<Long>() {
					@Override
					public void invoke(Long value) {
					}
				});

		return env;
	}
}
