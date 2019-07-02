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

package org.apache.flink.state.api;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * IT case for reading state.
 */
public class SavepointReaderKeyedStateITCase extends AbstractTestBase {
	private static final String uid = "stateful-operator";

	private static ValueStateDescriptor<Integer> valueState = new ValueStateDescriptor<>("value", Types.INT);

	@Test
	public void testKeyedInputFormat() throws Exception {
		runKeyedState(new MemoryStateBackend());
		// Reset the cluster so we can change the
		// state backend in the StreamEnvironment.
		// If we don't do this the tests will fail.
		miniClusterResource.after();
		miniClusterResource.before();
		runKeyedState(new RocksDBStateBackend((StateBackend) new MemoryStateBackend()));
	}

	private void runKeyedState(StateBackend backend) throws Exception {
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setStateBackend(backend);
		streamEnv.setParallelism(4);

		streamEnv
			.addSource(new SavepointSource())
			.rebalance()
			.keyBy(id -> id)
			.process(new KeyedStatefulOperator())
			.uid(uid)
			.addSink(new DiscardingSink<>());

		JobGraph jobGraph = streamEnv.getStreamGraph().getJobGraph();

		String path = takeSavepoint(jobGraph);

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, path, backend);

		List<Integer> results = savepoint
			.readKeyedState(uid, new Reader())
			.collect();

		results.sort(Comparator.naturalOrder());

		List<Integer> expected = SavepointSource.getElements();

		Assert.assertEquals("Unexpected results from keyed state", expected, results);
	}

	private String takeSavepoint(JobGraph jobGraph) throws Exception {
		SavepointSource.initializeForTest();

		ClusterClient<?> client = miniClusterResource.getClusterClient();
		client.setDetached(true);

		JobID jobId = jobGraph.getJobID();

		Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5));

		String dirPath = getTempDirPath(new AbstractID().toHexString());

		try {
			client.setDetached(true);
			JobSubmissionResult result = client.submitJob(jobGraph, getClass().getClassLoader());

			boolean finished = false;
			while (deadline.hasTimeLeft()) {
				if (SavepointSource.isFinished()) {
					finished = true;

					break;
				}
			}

			if (!finished) {
				Assert.fail("Failed to initialize state within deadline");
			}

			CompletableFuture<String> path = client.triggerSavepoint(result.getJobID(), dirPath);
			return path.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
		} finally {
			client.cancel(jobId);
		}
	}

	private static class SavepointSource implements SourceFunction<Integer> {
		private static volatile boolean finished;

		private volatile boolean running = true;

		private static final Integer[] elements = {1, 2, 3};

		@Override
		public void run(SourceContext<Integer> ctx) {
			synchronized (ctx.getCheckpointLock()) {
				for (Integer element : elements) {
					ctx.collect(element);
				}

				finished = true;
			}

			while (running) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// ignore
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		private static void initializeForTest() {
			finished = false;
		}

		private static boolean isFinished() {
			return finished;
		}

		private static List<Integer> getElements() {
			return Arrays.asList(elements);
		}
	}

	private static class KeyedStatefulOperator extends KeyedProcessFunction<Integer, Integer, Void> {

		private transient ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(valueState);
		}

		@Override
		public void processElement(Integer value, Context ctx, Collector<Void> out) throws Exception {
			state.update(value);
		}
	}

	private static class Reader extends KeyedStateReaderFunction<Integer, Integer> {

		private transient ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(valueState);
		}

		@Override
		public void readKey(Integer key, Context ctx, Collector<Integer> out) throws Exception {
			out.collect(state.value());
		}
	}
}
