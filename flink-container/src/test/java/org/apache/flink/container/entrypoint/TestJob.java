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

package org.apache.flink.container.entrypoint;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Test job which is used for {@link ClassPathJobGraphRetrieverTest}.
 */
public class TestJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);
		final SingleOutputStreamOperator<Integer> mapper = source.map(element -> 2 * element);
		mapper.addSink(new DiscardingSink<>());

		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		env.execute(TestJob.class.getCanonicalName() + "-" + parameterTool.getRequired("arg"));
	}

	/**
	 * Returns the test jar including {@link TestJob} (see pom.xml and assembly/test-assembly.xml).
	 *
	 * @return Test jar file
	 * @throws FileNotFoundException If test-jar can not be found
	 */
	static File getTestJobJar() throws FileNotFoundException {
		// Check the module's pom.xml for how we create the JAR
		File f = new File("target/maven-test-jar.jar");
		if (!f.exists()) {
			throw new FileNotFoundException("Test jar not present. Invoke tests using Maven "
				+ "or build the jar using 'mvn process-test-classes' in flink-container");
		}
		return f;
	}
}
