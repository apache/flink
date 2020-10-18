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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.SuccessException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.test.util.TestUtils.tryExecute;

/** Use RemoteInputChannel. Only for Upstream Reconnection. **/

public class ApproximateLocalRecoveryDownstreamITCase {
	private static MiniClusterWithClientResource cluster;

	@Before
	public void setup() throws Exception {
		Configuration config = new Configuration();
		// config.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "individual");
		config.setString(JobManagerOptions.SCHEDULING_STRATEGY, "legacy");
		config.setString(HighAvailabilityOptions.HA_MODE, RegionFailoverITCase.TestingHAFactory.class.getName());

		config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4096"));
		config.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 1000000L);
		config.setString(AkkaOptions.ASK_TIMEOUT, "1 h");
		//configuration.setString("heartbeat.timeout", "1000000");

		cluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(config)
				.setNumberTaskManagers(2)
				.setNumberSlotsPerTaskManager(2).build());
		cluster.before();
	}

	@AfterClass
	public static void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.after();
			cluster = null;
		}
	}

	@Test
	public void localTaskFailureRecovery3Tasks() throws Exception {
		int numElementsPerTask = 1000000;
		int producerParallelism = 1;
		int failAfterElements = 10;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
		env.setBufferTimeout(0);
		env.disableOperatorChaining();
		// env.setGlobalDataExchangeMode(GlobalDataExchangeMode.ALL_EDGES_PIPELINED_APPROXIMATE);
		// with APPROXIMATE mode but without checkpoint enabled.
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.APPROXIMATE);

		DataStream<Tuple4<Integer, Long, Integer, String>> source =
			// env.addSource(new AppSourceFunction(numElementsPerTask))
			env.addSource(new AppSourceFunction(numElementsPerTask, 4096))
				.setParallelism(producerParallelism)
				.slotSharingGroup("source");

		source.map(new FailingMapper<>(failAfterElements))
			.setParallelism(1)
			.slotSharingGroup("map")
			.addSink(new ValidatingAtMostOnceSink(100))
			.setParallelism(producerParallelism)
			.slotSharingGroup("sink");

		FailingMapper.failedBefore = false;
		tryExecute(env, "test");
	}

	@Test
	public void localTaskFailureRecovery2MapTasks() throws Exception {
		int numElementsPerTask = 1000000;
		int producerParallelism = 1;
		int failAfterElements = 10;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
		env.setBufferTimeout(0);
		env.disableOperatorChaining();
		// env.setGlobalDataExchangeMode(GlobalDataExchangeMode.ALL_EDGES_PIPELINED_APPROXIMATE);
		// with APPROXIMATE mode but without checkpoint enabled.
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.APPROXIMATE);

		DataStream<Tuple4<Integer, Long, Integer, String>> source =
			env.addSource(new AppSourceFunction(numElementsPerTask))
			// env.addSource(new AppSourceFunction(numElementsPerTask, 4096))
				.setParallelism(producerParallelism)
				.slotSharingGroup("source")
				.map(new FailingMapper<>(failAfterElements))
				.setParallelism(2)
				.slotSharingGroup("map");

			source.addSink(new ValidatingAtMostOnceSink(100))
			.setParallelism(producerParallelism)
			.slotSharingGroup("sink");

		FailingMapper.failedBefore = false;
		tryExecute(env, "test");
	}

	// Schema: (key, timestamp, source instance Id).
	private static class AppSourceFunction extends RichParallelSourceFunction<Tuple4<Integer, Long, Integer, String>>
		implements ListCheckpointed<Integer> {
		private static final Logger LOG = LoggerFactory.getLogger(AppSourceFunction.class);
		private volatile boolean running = true;
		private final int numElementsPerProducer;
		private final StringBuilder veryLongString;

		private int index = 0;

		AppSourceFunction(int numElementsPerProducer) {
			this.numElementsPerProducer = numElementsPerProducer;
			this.veryLongString = null;
		}

		AppSourceFunction(int numElementsPerProducer, int bufferSize) {
			this.numElementsPerProducer = numElementsPerProducer;
			String longString = "I am a very long string to test partial records hohoho hahaha";
			veryLongString = new StringBuilder(longString);

			for (int i = 0; i <= 2 * bufferSize / longString.length() + 1; i++) {
				veryLongString.append(longString);
			}
		}

		@Override
		public void run(SourceContext<Tuple4<Integer, Long, Integer, String>> ctx) throws Exception{
			long timestamp = 1593575900000L;
			int sourceInstanceId = getRuntimeContext().getIndexOfThisSubtask();
			for (; index < numElementsPerProducer && running; index++) {
				synchronized (ctx.getCheckpointLock()) {
					if (index % 100 == 0) {
						Thread.sleep(500);
					}
					System.out.println("Source : [" + index + "," + timestamp + "," + sourceInstanceId + "]");
					if (veryLongString == null) {
						ctx.collect(new Tuple4<>(index, timestamp++ % 2, sourceInstanceId, "I am a very long string to test partial records hohoho hahaha"));
					} else {
						ctx.collect(new Tuple4<>(index, timestamp++ % 2, sourceInstanceId, veryLongString.toString()));
					}
				}
			}
			while (running) {
				Thread.sleep(100);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.info("Snapshot of Source index " + index + " at checkpoint " + checkpointId);
			return Collections.singletonList(index);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}

			index = state.get(0);
			LOG.info("restore Source index to " + index);
		}
	}

	private static class FailingMapper<T> extends RichMapFunction<T, T> implements
		ListCheckpointed<Integer>, CheckpointListener, Runnable {

		private static final Logger LOG = LoggerFactory.getLogger(FailingMapper.class);

		private static final long serialVersionUID = 6334389850158707313L;

		public static volatile boolean failedBefore;

		private final int failCount;
		private int numElementsTotal;
		private int numElementsThisTime;

		private boolean failer;
		private boolean hasBeenCheckpointed;

		private Thread printer;
		private volatile boolean printerRunning = true;

		public FailingMapper(int failCount) {
			this.failCount = failCount;
		}

		@Override
		public void open(Configuration parameters) {
			failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
			printer = new Thread(this, "FailingIdentityMapper Status Printer");
			printer.start();
		}

		@Override
		public T map(T value) throws Exception {
			// System.out.println("Failing mapper: numElementsThisTime=" + numElementsThisTime + " totalCount=" + numElementsTotal);
			System.out.println("Failing mapper: [" + getRuntimeContext().getIndexOfThisSubtask() + "]" + value.toString());
			numElementsTotal++;
			numElementsThisTime++;

			if (!failedBefore) {
				Thread.sleep(10);

				if (failer && numElementsTotal >= failCount) {
					failedBefore = true;
					throw new Exception("Artificial Test Failure");
				}
			}
			return value;
		}

		@Override
		public void close() throws Exception {
			printerRunning = false;
			if (printer != null) {
				printer.interrupt();
				printer = null;
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			this.hasBeenCheckpointed = true;
		}

		@Override
		public void notifyCheckpointAborted(long checkpointId) {
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.info("Snapshot of FailingMapper numElementsTotal " + numElementsTotal + " at checkpoint " + checkpointId);
			return Collections.singletonList(numElementsTotal);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}

			this.numElementsTotal = state.get(0);
			LOG.info("restore FailingMapper numElementsTotal to " + numElementsTotal);
		}

		@Override
		public void run() {
			while (printerRunning) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// ignore
				}
				LOG.info("============================> Failing mapper  {}: count={}, totalCount={}",
					getRuntimeContext().getIndexOfThisSubtask(),
					numElementsThisTime, numElementsTotal);
			}
		}
	}

	private static class ValidatingAtMostOnceSink extends RichSinkFunction<Tuple4<Integer, Long, Integer, String>> implements ListCheckpointed<Tuple2<Integer, BitSet>> {

		private static final Logger LOG = LoggerFactory.getLogger(ValidatingAtMostOnceSink.class);

		private static final long serialVersionUID = 1748426382527469932L;

		private final int numElementsTotal;

		private BitSet duplicateChecker = new BitSet();  // this is checkpointed

		private int numElements; // this is checkpointed

		public ValidatingAtMostOnceSink(int numElementsTotal) {
			this.numElementsTotal = numElementsTotal;
		}

		@Override
		public void invoke(Tuple4<Integer, Long, Integer, String> value) throws Exception {
			numElements++;

			System.out.println("ValidatingAtMostOnceSink : [" + value.f0 + "]");

			if (duplicateChecker.get(value.f0)) {
				throw new Exception("Received a duplicate: " + value);
			}

			duplicateChecker.set(value.f0);
			if (numElements == numElementsTotal) {
				throw new SuccessException();
			}
		}

		@Override
		public List<Tuple2<Integer, BitSet>> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.info("Snapshot of ValidatingExactlyOnceSink numElements " + numElements + " at checkpoint " + checkpointId);
			return Collections.singletonList(new Tuple2<>(numElements, duplicateChecker));
		}

		@Override
		public void restoreState(List<Tuple2<Integer, BitSet>> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}

			Tuple2<Integer, BitSet> s = state.get(0);
			LOG.info("restoring ValidatingExactlyOnceSink num elements to {}", s.f0);
			this.numElements = s.f0;
			this.duplicateChecker = s.f1;
		}
	}
}
