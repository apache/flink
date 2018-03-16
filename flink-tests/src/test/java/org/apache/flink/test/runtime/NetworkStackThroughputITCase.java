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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Manually test the throughput of the network stack.
 */
@Ignore
public class NetworkStackThroughputITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkStackThroughputITCase.class);

	private static final String DATA_VOLUME_GB_CONFIG_KEY = "data.volume.gb";

	private static final String IS_SLOW_SENDER_CONFIG_KEY = "is.slow.sender";

	private static final String IS_SLOW_RECEIVER_CONFIG_KEY = "is.slow.receiver";

	private static final int IS_SLOW_SLEEP_MS = 10;

	private static final int IS_SLOW_EVERY_NUM_RECORDS = (2 * 32 * 1024) / SpeedTestRecord.RECORD_SIZE;

	// ------------------------------------------------------------------------

	/**
	 * Invokable that produces records and allows slowdown via {@link #IS_SLOW_EVERY_NUM_RECORDS}
	 * and {@link #IS_SLOW_SENDER_CONFIG_KEY} and creates records of different data sizes via {@link
	 * #DATA_VOLUME_GB_CONFIG_KEY}.
	 *
	 * <p>NOTE: needs to be <tt>public</tt> so that a task can be run with this!
	 */
	public static class SpeedTestProducer extends AbstractInvokable {

		public SpeedTestProducer(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			RecordWriter<SpeedTestRecord> writer = new RecordWriter<>(getEnvironment().getWriter(0));

			try {
				// Determine the amount of data to send per subtask
				int dataVolumeGb = getTaskConfiguration().getInteger(NetworkStackThroughputITCase.DATA_VOLUME_GB_CONFIG_KEY, 1);

				long dataMbPerSubtask = (dataVolumeGb * 1024) / getCurrentNumberOfSubtasks();
				long numRecordsToEmit = (dataMbPerSubtask * 1024 * 1024) / SpeedTestRecord.RECORD_SIZE;

				LOG.info(String.format("%d/%d: Producing %d records (each record: %d bytes, total: %.2f GB)",
						getIndexInSubtaskGroup() + 1, getCurrentNumberOfSubtasks(), numRecordsToEmit,
						SpeedTestRecord.RECORD_SIZE, dataMbPerSubtask / 1024.0));

				boolean isSlow = getTaskConfiguration().getBoolean(IS_SLOW_SENDER_CONFIG_KEY, false);

				int numRecords = 0;
				SpeedTestRecord record = new SpeedTestRecord();
				for (long i = 0; i < numRecordsToEmit; i++) {
					if (isSlow && (numRecords++ % IS_SLOW_EVERY_NUM_RECORDS) == 0) {
						Thread.sleep(IS_SLOW_SLEEP_MS);
					}

					writer.emit(record);
				}
			}
			finally {
				writer.flushAll();
			}
		}
	}

	/**
	 * Invokable that forwards incoming records.
	 *
	 * <p>NOTE: needs to be <tt>public</tt> so that a task can be run with this!
	 */
	public static class SpeedTestForwarder extends AbstractInvokable {

		public SpeedTestForwarder(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			RecordReader<SpeedTestRecord> reader = new RecordReader<>(
					getEnvironment().getInputGate(0),
					SpeedTestRecord.class,
					getEnvironment().getTaskManagerInfo().getTmpDirectories());

			RecordWriter<SpeedTestRecord> writer = new RecordWriter<>(getEnvironment().getWriter(0));

			try {
				SpeedTestRecord record;
				while ((record = reader.next()) != null) {
					writer.emit(record);
				}
			}
			finally {
				reader.clearBuffers();
				writer.flushAll();
			}
		}
	}

	/**
	 * Invokable that consumes incoming records and allows slowdown via {@link
	 * #IS_SLOW_EVERY_NUM_RECORDS}.
	 *
	 * <p>NOTE: needs to be <tt>public</tt> so that a task can be run with this!
	 */
	public static class SpeedTestConsumer extends AbstractInvokable {

		public SpeedTestConsumer(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			RecordReader<SpeedTestRecord> reader = new RecordReader<>(
					getEnvironment().getInputGate(0),
					SpeedTestRecord.class,
					getEnvironment().getTaskManagerInfo().getTmpDirectories());

			try {
				boolean isSlow = getTaskConfiguration().getBoolean(IS_SLOW_RECEIVER_CONFIG_KEY, false);

				int numRecords = 0;
				while (reader.next() != null) {
					if (isSlow && (numRecords++ % IS_SLOW_EVERY_NUM_RECORDS) == 0) {
						Thread.sleep(IS_SLOW_SLEEP_MS);
					}
				}
			}
			finally {
				reader.clearBuffers();
			}
		}
	}

	/**
	 * Record type for the speed test.
	 *
	 * <p>NOTE: needs to be <tt>public</tt> to allow deserialization!
	 */
	public static class SpeedTestRecord implements IOReadableWritable {

		private static final int RECORD_SIZE = 128;

		private final byte[] buf = new byte[RECORD_SIZE];

		public SpeedTestRecord() {
			for (int i = 0; i < RECORD_SIZE; ++i) {
				this.buf[i] = (byte) (i % 128);
			}
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.write(this.buf);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			in.readFully(this.buf);
		}
	}

	// ------------------------------------------------------------------------

	public void testThroughput() throws Exception {
		Object[][] configParams = new Object[][]{
				new Object[]{1, false, false, false, 4, 2},
				new Object[]{1, true, false, false, 4, 2},
				new Object[]{1, true, true, false, 4, 2},
				new Object[]{1, true, false, true, 4, 2},
				new Object[]{2, true, false, false, 4, 2},
				new Object[]{4, true, false, false, 4, 2},
				new Object[]{4, true, false, false, 8, 4},
		};

		for (Object[] p : configParams) {
			final int dataVolumeGb = (Integer) p[0];
			final boolean useForwarder = (Boolean) p[1];
			final boolean isSlowSender = (Boolean) p[2];
			final boolean isSlowReceiver = (Boolean) p[3];
			final int parallelism = (Integer) p[4];
			final int numSlotsPerTaskManager = (Integer) p[5];

			if (parallelism % numSlotsPerTaskManager != 0) {
				throw new RuntimeException("The test case defines a parallelism that is not a multiple of the slots per task manager.");
			}

			final int numTaskManagers = parallelism / numSlotsPerTaskManager;

			final LocalFlinkMiniCluster localFlinkMiniCluster = TestBaseUtils.startCluster(
				numTaskManagers,
				numSlotsPerTaskManager,
				false,
				false,
				true);

			try {
				System.out.println(Arrays.toString(p));
				testProgram(
					localFlinkMiniCluster,
					dataVolumeGb,
					useForwarder,
					isSlowSender,
					isSlowReceiver,
					parallelism);
			} finally {
				TestBaseUtils.stopCluster(localFlinkMiniCluster, FutureUtils.toFiniteDuration(TestingUtils.TIMEOUT()));
			}
		}
	}

	private void testProgram(
			LocalFlinkMiniCluster localFlinkMiniCluster,
			final int dataVolumeGb,
			final boolean useForwarder,
			final boolean isSlowSender,
			final boolean isSlowReceiver,
			final int parallelism) throws Exception {
		JobExecutionResult jer = localFlinkMiniCluster.submitJobAndWait(
			createJobGraph(
				dataVolumeGb,
				useForwarder,
				isSlowSender,
				isSlowReceiver,
				parallelism),
			false);

		long dataVolumeMbit = dataVolumeGb * 8192;
		long runtimeSecs = jer.getNetRuntime(TimeUnit.SECONDS);

		int mbitPerSecond = (int) (((double) dataVolumeMbit) / runtimeSecs);

		LOG.info(String.format("Test finished with throughput of %d MBit/s (runtime [secs]: %d, " +
			"data volume [gb/mbits]: %d/%d)", mbitPerSecond, runtimeSecs, dataVolumeGb, dataVolumeMbit));
	}

	private JobGraph createJobGraph(int dataVolumeGb, boolean useForwarder, boolean isSlowSender,
									boolean isSlowReceiver, int numSubtasks) {
		JobGraph jobGraph = new JobGraph("Speed Test");
		SlotSharingGroup sharingGroup = new SlotSharingGroup();

		JobVertex producer = new JobVertex("Speed Test Producer");
		jobGraph.addVertex(producer);
		producer.setSlotSharingGroup(sharingGroup);

		producer.setInvokableClass(SpeedTestProducer.class);
		producer.setParallelism(numSubtasks);
		producer.getConfiguration().setInteger(DATA_VOLUME_GB_CONFIG_KEY, dataVolumeGb);
		producer.getConfiguration().setBoolean(IS_SLOW_SENDER_CONFIG_KEY, isSlowSender);

		JobVertex forwarder = null;
		if (useForwarder) {
			forwarder = new JobVertex("Speed Test Forwarder");
			jobGraph.addVertex(forwarder);
			forwarder.setSlotSharingGroup(sharingGroup);

			forwarder.setInvokableClass(SpeedTestForwarder.class);
			forwarder.setParallelism(numSubtasks);
		}

		JobVertex consumer = new JobVertex("Speed Test Consumer");
		jobGraph.addVertex(consumer);
		consumer.setSlotSharingGroup(sharingGroup);

		consumer.setInvokableClass(SpeedTestConsumer.class);
		consumer.setParallelism(numSubtasks);
		consumer.getConfiguration().setBoolean(IS_SLOW_RECEIVER_CONFIG_KEY, isSlowReceiver);

		if (useForwarder) {
			forwarder.connectNewDataSetAsInput(producer, DistributionPattern.ALL_TO_ALL,
				ResultPartitionType.PIPELINED);
			consumer.connectNewDataSetAsInput(forwarder, DistributionPattern.ALL_TO_ALL,
				ResultPartitionType.PIPELINED);
		}
		else {
			consumer.connectNewDataSetAsInput(producer, DistributionPattern.ALL_TO_ALL,
				ResultPartitionType.PIPELINED);
		}

		return jobGraph;
	}

	private void runAllTests() throws Exception {
		testThroughput();

		System.out.println("Done.");
	}

	public static void main(String[] args) throws Exception {
		new NetworkStackThroughputITCase().runAllTests();
	}
}
