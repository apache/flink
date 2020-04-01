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

package org.apache.flink.client.program;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link PackagedProgramUtils}. The equivalent test for DataStream programs is in the
 * flink-streaming-java package because in Flink < 1.11 flink-clients does not depend on
 * flink-streaming-java.
 */
public class PackagedProgramUtilsTest {

	/**
	 * This tests whether configuration forwarding from a {@link Configuration} to the environment
	 * works.
	 */
	@Test
	public void testDataSetConfigurationForwarding() throws Exception {
		// we want to test forwarding with this config, ensure that the default is what we expect.
		assertThat(
				ExecutionEnvironment.getExecutionEnvironment().getConfig().isAutoTypeRegistrationDisabled(),
				is(false));

		PackagedProgram packagedProgram = PackagedProgram.newBuilder()
				.setEntryPointClassName(DataSetTestProgram.class.getName())
				.build();

		Configuration config = new Configuration();
		config.set(PipelineOptions.AUTO_TYPE_REGISTRATION, false);

		Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(
				packagedProgram,
				config,
				1 /* parallelism */,
				false /* suppress output */);

		ExecutionConfig executionConfig = ((Plan) pipeline).getExecutionConfig();

		assertThat(executionConfig.isAutoTypeRegistrationDisabled(), is(true));
	}

	/** Test program for the DataSet API. */
	public static class DataSetTestProgram {
		public static void main(String[] args) throws Exception {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.fromElements("hello").print();
			env.execute();
		}
	}
}
