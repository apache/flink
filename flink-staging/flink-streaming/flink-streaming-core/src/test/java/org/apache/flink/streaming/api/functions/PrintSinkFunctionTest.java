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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Tests for the {@link org.apache.flink.streaming.api.functions.sink.PrintSinkFunction}.
 */
public class PrintSinkFunctionTest<IN> extends RichSinkFunction<IN> {

	public PrintStream printStreamOriginal = System.out;

	public class printStreamMock extends PrintStream{

		public String result;

		public printStreamMock(OutputStream out) {
			super(out);
		}

		@Override
		public void println(String x) {
			this.result = x;
		}
	}

	private Environment envForPrefixNull = new Environment() {
		@Override
		public JobID getJobID() {
			return null;
		}

		@Override
		public JobVertexID getJobVertexId() {
			return null;
		}

		@Override
		public ExecutionAttemptID getExecutionId() {
			return null;
		}

		@Override
		public Configuration getTaskConfiguration() {
			return null;
		}

		@Override
		public TaskManagerRuntimeInfo getTaskManagerInfo() {
			return null;
		}

		@Override
		public Configuration getJobConfiguration() {
			return null;
		}

		@Override
		public int getNumberOfSubtasks() {
			return 0;
		}

		@Override
		public int getIndexInSubtaskGroup() {
			return 0;
		}

		@Override
		public InputSplitProvider getInputSplitProvider() {
			return null;
		}

		@Override
		public IOManager getIOManager() {
			return null;
		}

		@Override
		public MemoryManager getMemoryManager() {
			return null;
		}

		@Override
		public String getTaskName() {
			return null;
		}

		@Override
		public String getTaskNameWithSubtasks() {
			return null;
		}

		@Override
		public ClassLoader getUserClassLoader() {
			return null;
		}

		@Override
		public Map<String, Future<Path>> getDistributedCacheEntries() {
			return new Map<String, Future<Path>>() {
				@Override
				public int size() {
					return 0;
				}

				@Override
				public boolean isEmpty() {
					return false;
				}

				@Override
				public boolean containsKey(Object key) {
					return false;
				}

				@Override
				public boolean containsValue(Object value) {
					return false;
				}

				@Override
				public Future<Path> get(Object key) {
					return null;
				}

				@Override
				public Future<Path> put(String key, Future<Path> value) {
					return null;
				}

				@Override
				public Future<Path> remove(Object key) {
					return null;
				}

				@Override
				public void putAll(Map<? extends String, ? extends Future<Path>> m) {

				}

				@Override
				public void clear() {

				}

				@Override
				public Set<String> keySet() {
					return null;
				}

				@Override
				public Collection<Future<Path>> values() {
					return null;
				}

				@Override
				public Set<Entry<String, Future<Path>>> entrySet() {
					return null;
				}
			};
		}

		@Override
		public BroadcastVariableManager getBroadcastVariableManager() {
			return null;
		}

		@Override
		public AccumulatorRegistry getAccumulatorRegistry() {
			return null;
		}

		@Override
		public void acknowledgeCheckpoint(long checkpointId) {

		}

		@Override
		public void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state) {

		}

		@Override
		public ResultPartitionWriter getWriter(int index) {
			return null;
		}

		@Override
		public ResultPartitionWriter[] getAllWriters() {
			return new ResultPartitionWriter[0];
		}

		@Override
		public InputGate getInputGate(int index) {
			return null;
		}

		@Override
		public InputGate[] getAllInputGates() {
			return new InputGate[0];
		}
	};

	public OutputStream out = new OutputStream() {
		@Override
		public void write(int b) throws IOException {

		}
	};

	@Test
	public void testPrintSinkStdOut(){

		printStreamMock stream = new printStreamMock(out);
		System.setOut(stream);

		ExecutionConfig executionConfig = new ExecutionConfig();
		final HashMap<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();
		final StreamingRuntimeContext ctx = new StreamingRuntimeContext(
				this.envForPrefixNull,
				executionConfig,
				null,
				null,
				accumulators
		);

		PrintSinkFunction<String> printSink = new PrintSinkFunction<String>();
		printSink.setRuntimeContext(ctx);
		try {
			printSink.open(new Configuration());
		} catch (Exception e) {
			e.printStackTrace();
		}
		printSink.setTargetToStandardOut();
		printSink.invoke("hello world!");

		assertEquals("Print to System.out", printSink.toString());
		assertEquals("hello world!", stream.result);

		printSink.close();
	}

	@Test
	public void testPrintSinkStdErr(){

		printStreamMock stream = new printStreamMock(out);
		System.setOut(stream);

		ExecutionConfig executionConfig = new ExecutionConfig();
		final HashMap<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();
		final StreamingRuntimeContext ctx = new StreamingRuntimeContext(
				this.envForPrefixNull,
				executionConfig,
				null,
				null,
				accumulators
		);

		PrintSinkFunction<String> printSink = new PrintSinkFunction<String>();
		printSink.setRuntimeContext(ctx);
		try {
			printSink.open(new Configuration());
		} catch (Exception e) {
			e.printStackTrace();
		}
		printSink.setTargetToStandardErr();
		printSink.invoke("hello world!");

		assertEquals("Print to System.err", printSink.toString());
		assertEquals("hello world!", stream.result);

		printSink.close();
	}

	@Override
	public void invoke(IN record) {

	}

	@After
	public void restoreSystemOut() {
		System.setOut(printStreamOriginal);
	}

}