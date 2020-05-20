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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

import static org.apache.flink.client.program.PackagedProgramUtils.resolveURI;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link PackagedProgramUtils}.
 */
public class PackagedProgramUtilsTest {

	/**
	 * This tests whether configuration forwarding from a {@link Configuration} to the environment
	 * works.
	 */
	@Test
	public void testDataSetConfigurationForwarding() throws Exception {
		assertPrecondition(ExecutionEnvironment.getExecutionEnvironment().getConfig());

		PackagedProgram packagedProgram = PackagedProgram.newBuilder()
				.setEntryPointClassName(DataSetTestProgram.class.getName())
				.build();

		Configuration config = createConfigurationWithOption();

		Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(
				packagedProgram,
				config,
				1 /* parallelism */,
				false /* suppress output */);

		ExecutionConfig executionConfig = ((Plan) pipeline).getExecutionConfig();

		assertExpectedOption(executionConfig);
	}

	/**
	 * This tests whether configuration forwarding from a {@link Configuration} to the environment
	 * works.
	 */
	@Test
	public void testDataStreamConfigurationForwarding() throws Exception {
		assertPrecondition(ExecutionEnvironment.getExecutionEnvironment().getConfig());

		PackagedProgram packagedProgram = PackagedProgram.newBuilder()
				.setEntryPointClassName(DataStreamTestProgram.class.getName())
				.build();

		Configuration config = createConfigurationWithOption();

		Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(
				packagedProgram,
				config,
				1 /* parallelism */,
				false /* suppress output */);

		ExecutionConfig executionConfig = ((StreamGraph) pipeline).getExecutionConfig();

		assertExpectedOption(executionConfig);
	}

	@Test
	public void testResolveURI() throws URISyntaxException {
		final String relativeFile = "path/of/user.jar";
		assertThat(resolveURI(relativeFile).getScheme(), is("file"));
		assertThat(
			resolveURI(relativeFile).getPath(),
			is(new File(System.getProperty("user.dir"), relativeFile).getAbsolutePath()));

		final String absoluteFile = "/path/of/user.jar";
		assertThat(resolveURI(relativeFile).getScheme(), is("file"));
		assertThat(resolveURI(absoluteFile).getPath(), is(absoluteFile));

		final String fileSchemaFile = "file:///path/of/user.jar";
		assertThat(resolveURI(fileSchemaFile).getScheme(), is("file"));
		assertThat(resolveURI(fileSchemaFile).toString(), is(fileSchemaFile));

		final String localSchemaFile = "local:///path/of/user.jar";
		assertThat(resolveURI(localSchemaFile).getScheme(), is("local"));
		assertThat(resolveURI(localSchemaFile).toString(), is(localSchemaFile));
	}

	private static void assertPrecondition(ExecutionConfig executionConfig) {
		// we want to test forwarding with this config, ensure that the default is what we expect.
		assertThat(executionConfig.isAutoTypeRegistrationDisabled(), is(false));
	}

	private static void assertExpectedOption(ExecutionConfig executionConfig) {
		// we want to test forwarding with this config, ensure that the default is what we expect.
		assertThat(executionConfig.isAutoTypeRegistrationDisabled(), is(true));
	}

	private static Configuration createConfigurationWithOption() {
		Configuration config = new Configuration();
		config.set(PipelineOptions.AUTO_TYPE_REGISTRATION, false);
		return config;
	}

	/** Test Program for the DataSet API. */
	public static class DataSetTestProgram {
		public static void main(String[] args) throws Exception {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.fromElements("hello").print();
			env.execute();
		}
	}

	/** Test Program for the DataStream API. */
	public static class DataStreamTestProgram {
		public static void main(String[] args) throws Exception {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.fromElements("hello").print();
			env.execute();
		}
	}
}
