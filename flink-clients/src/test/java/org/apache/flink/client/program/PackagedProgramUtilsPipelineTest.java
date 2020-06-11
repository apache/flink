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
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.testutils.ClassLoaderUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link PackagedProgramUtils} methods that should be executed for
 * {@link StreamExecutionEnvironment} and {@link Environment}.
 */
@RunWith(Parameterized.class)
public class PackagedProgramUtilsPipelineTest {

	@Parameterized.Parameter
	public TestParameter testParameter;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Parameterized.Parameters
	public static Collection<TestParameter> parameters() {
		return Arrays.asList(
			TestParameter.of(DataSetTestProgram.class, pipeline -> ((Plan) pipeline).getExecutionConfig()),
			TestParameter.of(DataStreamTestProgram.class, pipeline -> ((StreamGraph) pipeline).getExecutionConfig())
		);
	}

	/**
	 * This tests whether configuration forwarding from a {@link Configuration} to the environment
	 * works.
	 */
	@Test
	public void testConfigurationForwarding() throws Exception {
		// we want to test forwarding with this config, ensure that the default is what we expect.
		assertThat(
			ExecutionEnvironment.getExecutionEnvironment().getConfig().isAutoTypeRegistrationDisabled(),
			is(false));

		PackagedProgram packagedProgram = PackagedProgram.newBuilder()
			.setEntryPointClassName(testParameter.entryClass().getName())
			.build();

		Configuration config = new Configuration();
		config.set(PipelineOptions.AUTO_TYPE_REGISTRATION, false);

		Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(
			packagedProgram,
			config,
			1 /* parallelism */,
			false /* suppress output */);

		ExecutionConfig executionConfig = testParameter.extractExecutionConfig(pipeline);

		// we want to test forwarding with this config, ensure that the default is what we expect.
		assertThat(executionConfig.isAutoTypeRegistrationDisabled(), is(true));
	}

	@Test
	public void testUserClassloaderForConfiguration() throws Exception {
		String userSerializerClassName = "UserSerializer";
		List<URL> userUrls = getClassUrls(userSerializerClassName);

		PackagedProgram packagedProgram = PackagedProgram.newBuilder()
			.setUserClassPaths(userUrls)
			.setEntryPointClassName(testParameter.entryClass().getName())
			.build();

		Configuration config = new Configuration();
		config.set(PipelineOptions.KRYO_DEFAULT_SERIALIZERS, Collections.singletonList(
			String.format(
				"class:%s,serializer:%s",
				PackagedProgramUtilsPipelineTest.class.getName(),
				userSerializerClassName)
		));

		Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(
			packagedProgram,
			config,
			1 /* parallelism */,
			false /* suppress output */);

		ExecutionConfig executionConfig = testParameter.extractExecutionConfig(pipeline);

		assertThat(
			executionConfig.getDefaultKryoSerializerClasses().get(PackagedProgramUtilsPipelineTest.class).getName(),
			is(userSerializerClassName));
	}

	private List<URL> getClassUrls(String className) throws IOException {
		URLClassLoader urlClassLoader = ClassLoaderUtils.compileAndLoadJava(
			temporaryFolder.newFolder(),
			className + ".java",
			"import com.esotericsoftware.kryo.Kryo;\n" +
				"import com.esotericsoftware.kryo.Serializer;\n" +
				"import com.esotericsoftware.kryo.io.Input;\n" +
				"import com.esotericsoftware.kryo.io.Output;\n"
				+ "public class " + className + " extends Serializer {\n" +
				"\t@Override\n" +
				"\tpublic void write(\n" +
				"\t\tKryo kryo,\n" +
				"\t\tOutput output,\n" +
				"\t\tObject object) {\n" +
				"\t}\n" +
				"\n" +
				"\t@Override\n" +
				"\tpublic Object read(Kryo kryo, Input input, Class type) {\n" +
				"\t\treturn null;\n" +
				"\t}\n" +
				"}");
		return Arrays.asList(urlClassLoader.getURLs());
	}

	private interface TestParameter {
		Class<?> entryClass();

		ExecutionConfig extractExecutionConfig(Pipeline pipeline);

		static TestParameter of(Class<?> entryClass, Function<Pipeline, ExecutionConfig> executionConfigExtractor) {
			return new TestParameter() {
				@Override
				public Class<?> entryClass() {
					return entryClass;
				}

				@Override
				public ExecutionConfig extractExecutionConfig(Pipeline pipeline) {
					return executionConfigExtractor.apply(pipeline);
				}
			};
		}
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
