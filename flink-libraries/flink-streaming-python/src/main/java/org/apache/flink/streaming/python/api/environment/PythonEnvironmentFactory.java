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

package org.apache.flink.streaming.python.api.environment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.LegacyLocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A factory for {@link PythonStreamExecutionEnvironment}s.
 *
 * <p>This class is a replacement for static factory methods defined in {@link StreamExecutionEnvironment} and allows
 * us to pass state from the {@link org.apache.flink.streaming.python.api.PythonStreamBinder} instance
 * to the created execution environment without having to rely on static fields.
 */
public class PythonEnvironmentFactory {
	private final String localTmpPath;
	private final String scriptName;

	public PythonEnvironmentFactory(String localTmpPath, String scriptName) {
		this.localTmpPath = localTmpPath;
		this.scriptName = scriptName;
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#getExecutionEnvironment()}. In addition it takes
	 * care for required Jython serializers registration.
	 *
	 * @return The python execution environment of the context in which the program is
	 * executed.
	 */
	public PythonStreamExecutionEnvironment get_execution_environment() {
		return new PythonStreamExecutionEnvironment(StreamExecutionEnvironment.getExecutionEnvironment(), new Path(localTmpPath), scriptName);
	}

	/**
	 * Creates a {@link LocalStreamEnvironment}. The local execution environment
	 * will run the program in a multi-threaded fashion in the same JVM as the
	 * environment was created in. The default parallelism of the local
	 * environment is the number of hardware contexts (CPU cores / threads),
	 * unless it was specified differently by {@link PythonStreamExecutionEnvironment#set_parallelism(int)}.
	 *
	 * @param config Pass a custom configuration into the cluster
	 * @return A local execution environment with the specified parallelism.
	 */
	public PythonStreamExecutionEnvironment create_local_execution_environment(Configuration config) {
		return new PythonStreamExecutionEnvironment(new LegacyLocalStreamEnvironment(config), new Path(localTmpPath), scriptName);
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#createLocalEnvironment(int, Configuration)}.
	 *
	 * @param parallelism The parallelism for the local environment.
	 * @param config Pass a custom configuration into the cluster
	 * @return A local python execution environment with the specified parallelism.
	 */
	public PythonStreamExecutionEnvironment create_local_execution_environment(int parallelism, Configuration config) {
		return new PythonStreamExecutionEnvironment(
			StreamExecutionEnvironment.createLocalEnvironment(parallelism, config), new Path(localTmpPath), scriptName);
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#createRemoteEnvironment(String, int, String...)}.
	 *
	 * @param host The host name or address of the master (JobManager), where the
	 * program should be executed.
	 * @param port The port of the master (JobManager), where the program should
	 * be executed.
	 * @param jar_files The JAR files with code that needs to be shipped to the
	 * cluster. If the program uses user-defined functions,
	 * user-defined input formats, or any libraries, those must be
	 * provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public PythonStreamExecutionEnvironment create_remote_execution_environment(
		String host, int port, String... jar_files) {
		return new PythonStreamExecutionEnvironment(
			StreamExecutionEnvironment.createRemoteEnvironment(host, port, jar_files), new Path(localTmpPath), scriptName);
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#createRemoteEnvironment(
	 *String, int, Configuration, String...)}.
	 *
	 * @param host The host name or address of the master (JobManager), where the
	 * program should be executed.
	 * @param port The port of the master (JobManager), where the program should
	 * be executed.
	 * @param config The configuration used by the client that connects to the remote cluster.
	 * @param jar_files The JAR files with code that needs to be shipped to the
	 * cluster. If the program uses user-defined functions,
	 * user-defined input formats, or any libraries, those must be
	 * provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public PythonStreamExecutionEnvironment create_remote_execution_environment(
		String host, int port, Configuration config, String... jar_files) {
		return new PythonStreamExecutionEnvironment(
			StreamExecutionEnvironment.createRemoteEnvironment(host, port, config, jar_files), new Path(localTmpPath), scriptName);
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#createRemoteEnvironment(
	 *String, int, int, String...)}.
	 *
	 * @param host The host name or address of the master (JobManager), where the
	 * program should be executed.
	 * @param port The port of the master (JobManager), where the program should
	 * be executed.
	 * @param parallelism The parallelism to use during the execution.
	 * @param jar_files The JAR files with code that needs to be shipped to the
	 * cluster. If the program uses user-defined functions,
	 * user-defined input formats, or any libraries, those must be
	 * provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public PythonStreamExecutionEnvironment create_remote_execution_environment(
		String host, int port, int parallelism, String... jar_files) {
		return new PythonStreamExecutionEnvironment(
			StreamExecutionEnvironment.createRemoteEnvironment(host, port, parallelism, jar_files), new Path(localTmpPath), scriptName);
	}
}
