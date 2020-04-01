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

package org.apache.flink.streaming.util;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link PackagedProgramUtils}. Normally, this should be in flink-clients, but in Flink <
 * 1.11 flink-clients does not depend on flink-streaming-java so we have to do the test here. There
 * is another {@code PackagedProgramUtilsTest} in flink-clients for the DataSet API. On Flink >=
 * 1.11 both tests are in the same class in flink-clients.
 */
public class PackagedProgramUtilsTest {

	/**
	 * This tests whether configuration forwarding from a {@link Configuration} to the environment
	 * works.
	 */
	@Test
	public void testDataStreamConfigurationForwarding() throws Exception {
		// we want to test forwarding with this config, ensure that the default is what we expect.
		assertThat(
				StreamExecutionEnvironment.getExecutionEnvironment().getCheckpointConfig().getCheckpointInterval(),
				is(-1L));

		PackagedProgram packagedProgram = PackagedProgram.newBuilder()
				.setEntryPointClassName(DataStreamTestProgram.class.getName())
				.build();

		Configuration config = new Configuration();
		config.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(100));

		Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(
				packagedProgram,
				config,
				1 /* parallelism */,
				false /* suppress output */);

		CheckpointConfig checkpointConfig = ((StreamGraph) pipeline).getCheckpointConfig();

		assertThat(checkpointConfig.getCheckpointInterval(), is(100L));
		assertThat(checkpointConfig.isCheckpointingEnabled(), is(true));
	}

	/** Test program for the DataStream API. */
	public static class DataStreamTestProgram {
		public static void main(String[] args) throws Exception {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.fromElements("hello").print();
			env.execute();
		}
	}
}
