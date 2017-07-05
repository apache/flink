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

package org.apache.flink.benchmark;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Base class for all JMH benchmarks running Flink jobs.
 */
@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(Throughput)
@Fork(value = 3, jvmArgsAppend = {
		"-Djava.rmi.server.hostname=127.0.0.1",
		"-Dcom.sun.management.jmxremote.authenticate=false",
		"-Dcom.sun.management.jmxremote.ssl=false"})
@OperationsPerInvocation(value = BenchmarkBase.RECORDS_PER_INVOCATION)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class BenchmarkBase {

	public static final int RECORDS_PER_INVOCATION = 7_000_000;

	@State(Thread)
	public static class Context {

		@Param({"1"})
		protected int parallelism = 1;

		@Param({"true"})
		protected boolean objectReuse = true;

		/**
		 * Other allowed values are "memory", "fs", "fsAsync", "rocks", "rocksIncremental" although
		 */
		@Param({"memory", "rocks"})
		protected String stateBackend = "memory";

		protected final File checkpointDir;

		protected final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		public Context() {
			try {
				checkpointDir = Files.createTempDirectory("bench-").toFile();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public StreamExecutionEnvironment getEnv() {
			return env;
		}

		@Setup
		public void setUp() throws IOException {
			// set up the execution environment
			env.setParallelism(parallelism);
			env.getConfig().disableSysoutLogging();
			if (objectReuse) {
				env.getConfig().enableObjectReuse();
			}

			final AbstractStateBackend stateBackend;
			String checkpointDataUri = "file://" + checkpointDir.getAbsolutePath();
			switch (this.stateBackend) {
				case "memory":
					stateBackend = new MemoryStateBackend();
					break;
				case "fs":
					stateBackend = new FsStateBackend(checkpointDataUri, false);
					break;
				case "fsAsync":
					stateBackend = new FsStateBackend(checkpointDataUri, true);
					break;
				case "rocks":
					stateBackend = new RocksDBStateBackend(checkpointDataUri, false);
					break;
				case "rocksIncremental":
					stateBackend = new RocksDBStateBackend(checkpointDataUri, true);
					break;
				default:
					throw new IllegalArgumentException("Unknown state backend: " + this.stateBackend);
			}

			env.setStateBackend(stateBackend);
		}

		@TearDown
		public void tearDown() throws IOException {
			FileUtils.deleteDirectory(checkpointDir);
		}
	}
}
