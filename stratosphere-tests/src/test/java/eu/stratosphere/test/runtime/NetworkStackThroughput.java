/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.runtime;

import eu.stratosphere.api.common.io.GenericInputFormat;
import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.nephele.jobgraph.DistributionPattern;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.runtime.io.api.RecordReader;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.runtime.io.channels.ChannelType;
import eu.stratosphere.test.util.RecordAPITestBase;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.LogUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NetworkStackThroughput {

	private static final Log LOG = LogFactory.getLog(NetworkStackThroughput.class);

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

			setNumTaskTracker(parallelism / numSlots);
			setTaskManagerNumSlots(numSlots);

			LogUtils.initializeDefaultConsoleLogger();
		}

		@Override
		protected JobGraph getJobGraph() throws Exception {
			return createJobGraph(dataVolumeGb, useForwarder, isSlowSender, isSlowReceiver, parallelism);
		}

		private JobGraph createJobGraph(int dataVolumeGb, boolean useForwarder, boolean isSlowSender,
				boolean isSlowReceiver, int numSubtasks) throws JobGraphDefinitionException {

			JobGraph jobGraph = new JobGraph("Speed Test");

			JobInputVertex producer = new JobInputVertex("Speed Test Producer", jobGraph);
			producer.setInvokableClass(SpeedTestProducer.class);
			producer.setNumberOfSubtasks(numSubtasks);
			producer.getConfiguration().setInteger(DATA_VOLUME_GB_CONFIG_KEY, dataVolumeGb);
			producer.getConfiguration().setBoolean(IS_SLOW_SENDER_CONFIG_KEY, isSlowSender);

			TaskConfig inputConfig = new TaskConfig(producer.getConfiguration());
			inputConfig.setStubWrapper(new UserCodeObjectWrapper<Object>(new DummyInputFormat()));

			JobTaskVertex forwarder = null;
			if (useForwarder) {
				forwarder = new JobTaskVertex("Speed Test Forwarder", jobGraph);
				forwarder.setInvokableClass(SpeedTestForwarder.class);
				forwarder.setNumberOfSubtasks(numSubtasks);
			}

			JobOutputVertex consumer = new JobOutputVertex("Speed Test Consumer", jobGraph);
			consumer.setInvokableClass(SpeedTestConsumer.class);
			consumer.setNumberOfSubtasks(numSubtasks);
			consumer.getConfiguration().setBoolean(IS_SLOW_RECEIVER_CONFIG_KEY, isSlowReceiver);

			TaskConfig outputConfig = new TaskConfig(consumer.getConfiguration());
			outputConfig.setStubWrapper(new UserCodeObjectWrapper<Object>(new DummyOutputFormat()));

			if (useForwarder) {
				producer.connectTo(forwarder, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
				forwarder.connectTo(consumer, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

				forwarder.setVertexToShareInstancesWith(producer);
				consumer.setVertexToShareInstancesWith(producer);
			}
			else {
				producer.connectTo(consumer, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
				producer.setVertexToShareInstancesWith(consumer);
			}

			return jobGraph;
		}

		@After
		public void calculateThroughput() {
			if (getJobExecutionResult() != null) {
				int dataVolumeGb = this.config.getInteger(DATA_VOLUME_GB_CONFIG_KEY, 1);

				double dataVolumeMbit = dataVolumeGb * 8192.0;
				double runtimeSecs = getJobExecutionResult().getNetRuntime() / 1000.0;

				int mbitPerSecond = (int) Math.round(dataVolumeMbit / runtimeSecs);

				LOG.info(String.format("Test finished with throughput of %d MBit/s (runtime [secs]: %.2f, " +
								"data volume [mbits]: %.2f)", mbitPerSecond, runtimeSecs, dataVolumeMbit));
			}
		}
	}

	// ------------------------------------------------------------------------

	public static class SpeedTestProducer extends AbstractInvokable {

		private RecordWriter<SpeedTestRecord> writer;

		@Override
		public void registerInputOutput() {
			this.writer = new RecordWriter<SpeedTestRecord>(this);
		}

		@Override
		public void invoke() throws Exception {
			this.writer.initializeSerializers();

			// Determine the amount of data to send per subtask
			int dataVolumeGb = getTaskConfiguration().getInteger(NetworkStackThroughput.DATA_VOLUME_GB_CONFIG_KEY, 1);

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
			this.reader = new RecordReader<SpeedTestRecord>(this, SpeedTestRecord.class);
			this.writer = new RecordWriter<SpeedTestRecord>(this);
		}

		@Override
		public void invoke() throws Exception {
			this.writer.initializeSerializers();

			SpeedTestRecord record;
			while ((record = this.reader.next()) != null) {
				this.writer.emit(record);
			}

			this.writer.flush();
		}
	}

	public static class SpeedTestConsumer extends AbstractInvokable {

		private RecordReader<SpeedTestRecord> reader;

		@Override
		public void registerInputOutput() {
			this.reader = new RecordReader<SpeedTestRecord>(this, SpeedTestRecord.class);
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
		public void write(DataOutput out) throws IOException {
			out.write(this.buf);
		}

		@Override
		public void read(DataInput in) throws IOException {
			in.readFully(this.buf);
		}
	}

	public static final class DummyInputFormat extends GenericInputFormat {

		private static final long serialVersionUID = 6891640958330871924L;

		@Override
		public void open(InputSplit split) throws IOException {

		}

		@Override
		public boolean reachedEnd() throws IOException {
			return false;
		}

		@Override
		public Object nextRecord(Object reuse) throws IOException {
			return null;
		}
	}

	public static final class DummyOutputFormat implements OutputFormat<Record> {

		@Override
		public void configure(Configuration parameters) {
		}

		@Override
		public void open(int taskNumber, int numTasks) {
		}

		@Override
		public void writeRecord(Record record) {
		}

		@Override
		public void close() {
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
				new Object[]{4, true, false, false, 16, 8},
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

			test.startCluster();
			test.testJob();
			test.calculateThroughput();
			test.stopCluster();
		}
	}

	private void runAllTests() throws Exception {
		testThroughput();

		System.out.println("Done.");
	}

	public static void main(String[] args) throws Exception {
		new NetworkStackThroughput().runAllTests();
	}
}
