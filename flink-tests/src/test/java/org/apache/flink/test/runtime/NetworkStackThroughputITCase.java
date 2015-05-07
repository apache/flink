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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.test.util.RecordAPITestBase;
import org.junit.After;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Ignore
public class NetworkStackThroughputITCase {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkStackThroughputITCase.class);

	private static final String DATA_VOLUME_GB_CONFIG_KEY = "data.volume.gb";

	private static final String USE_FORWARDER_CONFIG_KEY = "use.forwarder";

	private static final String PARALLELISM_CONFIG_KEY = "num.subtasks";

	private static final String NUM_SLOTS_PER_TM_CONFIG_KEY = "num.slots.per.tm";

	private static final String IS_SLOW_SENDER_CONFIG_KEY = "is.slow.sender";

	private static final String IS_SLOW_RECEIVER_CONFIG_KEY = "is.slow.receiver";

	private static final int IS_SLOW_SLEEP_MS = 10;

	private static final int IS_SLOW_EVERY_NUM_RECORDS = (2 * 32 * 1024) / SpeedTestRecord.RECORD_SIZE;

	// ------------------------------------------------------------------------

	// wrapper to reuse RecordAPITestBase code in runs via main()
	private static class TestBaseWrapper extends RecordAPITestBase {

		private int dataVolumeGb;
		private boolean useForwarder;
		private boolean isSlowSender;
		private boolean isSlowReceiver;
		private int parallelism;

		public TestBaseWrapper(Configuration config) {
			super(config);

			dataVolumeGb = config.getInteger(DATA_VOLUME_GB_CONFIG_KEY, 1);
			useForwarder = config.getBoolean(USE_FORWARDER_CONFIG_KEY, true);
			isSlowSender = config.getBoolean(IS_SLOW_SENDER_CONFIG_KEY, false);
			isSlowReceiver = config.getBoolean(IS_SLOW_RECEIVER_CONFIG_KEY, false);
			parallelism = config.getInteger(PARALLELISM_CONFIG_KEY, 1);

			int numSlots = config.getInteger(NUM_SLOTS_PER_TM_CONFIG_KEY, 1);

			if (parallelism % numSlots != 0) {
				throw new RuntimeException("The test case defines a parallelism that is not a multiple of the slots per task manager.");
			}

			setNumTaskManagers(parallelism / numSlots);
			setTaskManagerNumSlots(numSlots);
		}

		@Override
		protected JobGraph getJobGraph() throws Exception {
			return createJobGraph(dataVolumeGb, useForwarder, isSlowSender, isSlowReceiver, parallelism);
		}

		private JobGraph createJobGraph(int dataVolumeGb, boolean useForwarder, boolean isSlowSender,
										boolean isSlowReceiver, int numSubtasks) {
			JobGraph jobGraph = new JobGraph("Speed Test");
			SlotSharingGroup sharingGroup = new SlotSharingGroup();

			AbstractJobVertex producer = new AbstractJobVertex("Speed Test Producer");
			jobGraph.addVertex(producer);
			producer.setSlotSharingGroup(sharingGroup);

			producer.setInvokableClass(SpeedTestProducer.class);
			producer.setParallelism(numSubtasks);
			producer.getConfiguration().setInteger(DATA_VOLUME_GB_CONFIG_KEY, dataVolumeGb);
			producer.getConfiguration().setBoolean(IS_SLOW_SENDER_CONFIG_KEY, isSlowSender);

			AbstractJobVertex forwarder = null;
			if (useForwarder) {
				forwarder = new AbstractJobVertex("Speed Test Forwarder");
				jobGraph.addVertex(forwarder);
				forwarder.setSlotSharingGroup(sharingGroup);

				forwarder.setInvokableClass(SpeedTestForwarder.class);
				forwarder.setParallelism(numSubtasks);
			}

			AbstractJobVertex consumer = new AbstractJobVertex("Speed Test Consumer");
			jobGraph.addVertex(consumer);
			consumer.setSlotSharingGroup(sharingGroup);

			consumer.setInvokableClass(SpeedTestConsumer.class);
			consumer.setParallelism(numSubtasks);
			consumer.getConfiguration().setBoolean(IS_SLOW_RECEIVER_CONFIG_KEY, isSlowReceiver);

			if (useForwarder) {
				forwarder.connectNewDataSetAsInput(producer, DistributionPattern.ALL_TO_ALL);
				consumer.connectNewDataSetAsInput(forwarder, DistributionPattern.ALL_TO_ALL);
			}
			else {
				consumer.connectNewDataSetAsInput(producer, DistributionPattern.ALL_TO_ALL);
			}

			return jobGraph;
		}

		@After
		public void calculateThroughput() {
			if (getJobExecutionResult() != null) {
				int dataVolumeGb = this.config.getInteger(DATA_VOLUME_GB_CONFIG_KEY, 1);

				long dataVolumeMbit = dataVolumeGb * 8192;
				long runtimeSecs = getJobExecutionResult().getNetRuntime(TimeUnit.SECONDS);

				int mbitPerSecond = (int) (((double) dataVolumeMbit) / runtimeSecs);

				LOG.info(String.format("Test finished with throughput of %d MBit/s (runtime [secs]: %d, " +
						"data volume [gb/mbits]: %d/%d)", mbitPerSecond, runtimeSecs, dataVolumeGb, dataVolumeMbit));
			}
		}
	}

	// ------------------------------------------------------------------------

	public static class SpeedTestProducer extends AbstractInvokable {

		private RecordWriter<SpeedTestRecord> writer;

		@Override
		public void registerInputOutput() {
			this.writer = new RecordWriter<SpeedTestRecord>(getEnvironment().getWriter(0));
		}

		@Override
		public void invoke() throws Exception {
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

				this.writer.emit(record);
			}

			this.writer.flush();
		}
	}

	public static class SpeedTestForwarder extends AbstractInvokable {

		private RecordReader<SpeedTestRecord> reader;

		private RecordWriter<SpeedTestRecord> writer;

		@Override
		public void registerInputOutput() {
			this.reader = new RecordReader<SpeedTestRecord>(getEnvironment().getInputGate(0), SpeedTestRecord.class);
			this.writer = new RecordWriter<SpeedTestRecord>(getEnvironment().getWriter(0));
		}

		@Override
		public void invoke() throws Exception {
			SpeedTestRecord record;
			while ((record = this.reader.next()) != null) {
				this.writer.emit(record);
			}

			this.reader.clearBuffers();
			this.writer.flush();
		}
	}

	public static class SpeedTestConsumer extends AbstractInvokable {

		private RecordReader<SpeedTestRecord> reader;

		@Override
		public void registerInputOutput() {
			this.reader = new RecordReader<SpeedTestRecord>(getEnvironment().getInputGate(0), SpeedTestRecord.class);
		}

		@Override
		public void invoke() throws Exception {
			boolean isSlow = getTaskConfiguration().getBoolean(IS_SLOW_RECEIVER_CONFIG_KEY, false);

			int numRecords = 0;
			while (this.reader.next() != null) {
				if (isSlow && (numRecords++ % IS_SLOW_EVERY_NUM_RECORDS) == 0) {
					Thread.sleep(IS_SLOW_SLEEP_MS);
				}
			}

			this.reader.clearBuffers();
		}
	}

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
			Configuration config = new Configuration();
			config.setInteger(DATA_VOLUME_GB_CONFIG_KEY, (Integer) p[0]);
			config.setBoolean(USE_FORWARDER_CONFIG_KEY, (Boolean) p[1]);
			config.setBoolean(IS_SLOW_SENDER_CONFIG_KEY, (Boolean) p[2]);
			config.setBoolean(IS_SLOW_RECEIVER_CONFIG_KEY, (Boolean) p[3]);
			config.setInteger(PARALLELISM_CONFIG_KEY, (Integer) p[4]);
			config.setInteger(NUM_SLOTS_PER_TM_CONFIG_KEY, (Integer) p[5]);

			TestBaseWrapper test = new TestBaseWrapper(config);

			System.out.println(Arrays.toString(p));
			test.testJob();
			test.calculateThroughput();
		}
	}

	private void runAllTests() throws Exception {
		testThroughput();

		System.out.println("Done.");
	}

	public static void main(String[] args) throws Exception {
		new NetworkStackThroughputITCase().runAllTests();
	}
}
