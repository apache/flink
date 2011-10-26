/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.checkpointing;

import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobGenericInputVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This integration test checks Nephele's fault tolerance capabilities by a series of jobs. The jobs feature different
 * connecting patterns and fail at different tasks in the processing pipeline.
 * 
 * @author warneke
 */
public class FailingJobITCase {

	/**
	 * The directory containing the Nephele configuration for this integration test.
	 */
	private static final String CONFIGURATION_DIRECTORY = "correct-conf";

	/**
	 * The number of records to generate by each producer.
	 */
	private static final int RECORDS_TO_GENERATE = 512 * 1024;

	/**
	 * The size of an individual record in bytes.
	 */
	private static final int RECORD_SIZE = 256;

	/**
	 * Configuration key to access the number of records after which the execution failure shall occur.
	 */
	private static final String FAILED_AFTER_RECORD_KEY = "failure.after.record";

	/**
	 * The degree of parallelism for the job.
	 */
	private static final int DEGREE_OF_PARALLELISM = 4;

	/**
	 * The key to access the index of the subtask which is supposed to fail.
	 */
	private static final String FAILURE_INDEX_KEY = "failure.index";

	/**
	 * The thread running the job manager.
	 */
	private static JobManagerThread jobManagerThread = null;

	/**
	 * The configuration for the job client;
	 */
	private static Configuration configuration;

	/**
	 * Global flag to indicate if a task has already failed once.
	 */
	private static final Set<String> FAILED_ONCE = new HashSet<String>();

	/**
	 * This is an auxiliary class to run the job manager thread.
	 * 
	 * @author warneke
	 */
	private static final class JobManagerThread extends Thread {

		/**
		 * The job manager instance.
		 */
		private final JobManager jobManager;

		/**
		 * Constructs a new job manager thread.
		 * 
		 * @param jobManager
		 *        the job manager to run in this thread.
		 */
		private JobManagerThread(final JobManager jobManager) {

			this.jobManager = jobManager;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void run() {

			// Run task loop
			this.jobManager.runTaskLoop();

			// Shut down
			this.jobManager.shutdown();
		}

		/**
		 * Checks whether the encapsulated job manager is completely shut down.
		 * 
		 * @return <code>true</code> if the encapsulated job manager is completely shut down, <code>false</code>
		 *         otherwise
		 */
		public boolean isShutDown() {

			return this.jobManager.isShutDown();
		}
	}

	/**
	 * Sets up Nephele in local mode.
	 */
	@BeforeClass
	public static void startNephele() {

		if (jobManagerThread == null) {

			// Create the job manager
			JobManager jobManager = null;

			try {

				// Try to find the correct configuration directory
				final String userDir = System.getProperty("user.dir");
				String configDir = userDir + File.separator + CONFIGURATION_DIRECTORY;
				if (!new File(configDir).exists()) {
					configDir = userDir + "/src/test/resources/" + CONFIGURATION_DIRECTORY;
				}
				
				final Constructor<JobManager> c = JobManager.class.getDeclaredConstructor(new Class[] { String.class,
					String.class });
				c.setAccessible(true);
				jobManager = c.newInstance(new Object[] { configDir,
					new String("local") });

			} catch (SecurityException e) {
				fail(e.getMessage());
			} catch (NoSuchMethodException e) {
				fail(e.getMessage());
			} catch (IllegalArgumentException e) {
				fail(e.getMessage());
			} catch (InstantiationException e) {
				fail(e.getMessage());
			} catch (IllegalAccessException e) {
				fail(e.getMessage());
			} catch (InvocationTargetException e) {
				fail(e.getMessage());
			}

			configuration = GlobalConfiguration
				.getConfiguration(new String[] { ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY });

			// Start job manager thread
			if (jobManager != null) {
				jobManagerThread = new JobManagerThread(jobManager);
				jobManagerThread.start();
			}

			// Wait for the local task manager to arrive
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * Shuts Nephele down.
	 */
	@AfterClass
	public static void stopNephele() {

		if (jobManagerThread != null) {
			jobManagerThread.interrupt();

			while (!jobManagerThread.isShutDown()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException i) {
					break;
				}
			}
		}
	}

	public static class FailingJobRecord implements Record {

		private final byte[] data = new byte[RECORD_SIZE];

		public FailingJobRecord() {
			for (int i = 0; i < this.data.length; ++i) {
				this.data[i] = (byte) (i % 31);
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void write(final DataOutput out) throws IOException {

			out.write(this.data);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void read(final DataInput in) throws IOException {

			in.readFully(this.data);
		}
	}

	public final static class InputTask extends AbstractGenericInputTask {

		private RecordWriter<FailingJobRecord> recordWriter;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<FailingJobRecord>(this, FailingJobRecord.class);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			boolean failing = false;

			final int failAfterRecord = getRuntimeConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getRuntimeConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
			}

			final FailingJobRecord record = new FailingJobRecord();
			for (int i = 0; i < RECORDS_TO_GENERATE; ++i) {
				this.recordWriter.emit(record);

				if (i == failAfterRecord && failing) {
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}
			}
		}
	}

	public final static class InnerTask extends AbstractTask {

		private MutableRecordReader<FailingJobRecord> recordReader;

		private RecordWriter<FailingJobRecord> recordWriter;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<FailingJobRecord>(this, FailingJobRecord.class);
			this.recordReader = new MutableRecordReader<FailingJobITCase.FailingJobRecord>(this,
				new BipartiteDistributionPattern());
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			final FailingJobRecord record = new FailingJobRecord();

			boolean failing = false;

			final int failAfterRecord = getRuntimeConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getRuntimeConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
			}

			int count = 0;

			while (this.recordReader.next(record)) {

				this.recordWriter.emit(record);
				if (count++ == failAfterRecord && failing) {
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}
			}
		}
	}
	public final static class RefailingInnerTask extends AbstractTask {

		private MutableRecordReader<FailingJobRecord> recordReader;

		private RecordWriter<FailingJobRecord> recordWriter;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<FailingJobRecord>(this, FailingJobRecord.class);
			this.recordReader = new MutableRecordReader<FailingJobITCase.FailingJobRecord>(this,
				new BipartiteDistributionPattern());
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			final FailingJobRecord record = new FailingJobRecord();

			boolean failing = false;

			final int failAfterRecord = getRuntimeConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
				failing = (getIndexInSubtaskGroup() == getRuntimeConfiguration().getInteger(FAILURE_INDEX_KEY, -1));
			

			int count = 0;

			while (this.recordReader.next(record)) {

				this.recordWriter.emit(record);
				if (count++ == failAfterRecord && failing) {
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}
			}
		}
	}
	public static final class OutputTask extends AbstractOutputTask {

		private MutableRecordReader<FailingJobRecord> recordReader;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordReader = new MutableRecordReader<FailingJobITCase.FailingJobRecord>(this,
				new BipartiteDistributionPattern());
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			boolean failing = false;

			final int failAfterRecord = getRuntimeConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getRuntimeConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
			}

			int count = 0;

			final FailingJobRecord record = new FailingJobRecord();
			while (this.recordReader.next(record)) {

				if (count++ == failAfterRecord && failing) {

					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}
			}
		}

	}

	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a failing inner vertex.
	 */
	@Test
	public void testFailingInternalVertex() {

		final JobGraph jobGraph = new JobGraph("Job with failing inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(InnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex2.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 95490);
		innerVertex2.getConfiguration().setInteger(FAILURE_INDEX_KEY, 0);

		final JobTaskVertex innerVertex3 = new JobTaskVertex("Inner vertex 3", jobGraph);
		innerVertex3.setTaskClass(InnerTask.class);
		innerVertex3.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex3.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		innerVertex2.setVertexToShareInstancesWith(input);
		innerVertex3.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(innerVertex2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex2.connectTo(innerVertex3, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex3.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Reset the FAILED_ONCE flags
		synchronized (FAILED_ONCE) {
			FAILED_ONCE.clear();
		}

		// Create job client and launch job
		try {
			JobClient jobClient = new JobClient(jobGraph, configuration);
			jobClient.submitJobAndWait();
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (JobExecutionException e) {
			fail(StringUtils.stringifyException(e));
		}
	}

	/**
	 * This test checks Nephele's capabilities to recover from file channel checkpoints.
	 */
	@Test
	public void testRecoveryFromFileChannels() {

		final JobGraph jobGraph = new JobGraph("Job with file channels");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		output.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 153201);
		output.getConfiguration().setInteger(FAILURE_INDEX_KEY, 1);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(output, ChannelType.FILE, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Reset the FAILED_ONCE flags
		synchronized (FAILED_ONCE) {
			FAILED_ONCE.clear();
		}

		// Create job client and launch job
		try {
			JobClient jobClient = new JobClient(jobGraph, configuration);
			jobClient.submitJobAndWait();
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (JobExecutionException e) {
			fail(StringUtils.stringifyException(e));
		}
	}
	/**
	 * This test checks Nephele's capabilities to recover from network channel checkpoints.
	 */
	@Test
	public void testRecoveryFromNetworkChannels() {

		final JobGraph jobGraph = new JobGraph("Job with Network channels");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		
		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(InnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex2.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 95490);
		innerVertex2.getConfiguration().setInteger(FAILURE_INDEX_KEY, 0);
		
		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		innerVertex2.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(innerVertex2, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
			innerVertex2.connectTo(output, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Reset the FAILED_ONCE flags
		synchronized (FAILED_ONCE) {
			FAILED_ONCE.clear();
		}

		// Create job client and launch job
		try {
			JobClient jobClient = new JobClient(jobGraph, configuration);
			jobClient.submitJobAndWait();
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (JobExecutionException e) {
			fail(StringUtils.stringifyException(e));
		}
	}
	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a failing input vertex.
	 */
	@Test
	public void testFailingInputVertex() {

		final JobGraph jobGraph = new JobGraph("Job with failing inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		input.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 804937);
		input.getConfiguration().setInteger(FAILURE_INDEX_KEY, 3);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(InnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		

		final JobTaskVertex innerVertex3 = new JobTaskVertex("Inner vertex 3", jobGraph);
		innerVertex3.setTaskClass(InnerTask.class);
		innerVertex3.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex3.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		innerVertex2.setVertexToShareInstancesWith(input);
		innerVertex3.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(innerVertex2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex2.connectTo(innerVertex3, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex3.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Reset the FAILED_ONCE flags
		synchronized (FAILED_ONCE) {
			FAILED_ONCE.clear();
		}

		// Create job client and launch job
		try {
			JobClient jobClient = new JobClient(jobGraph, configuration);
			jobClient.submitJobAndWait();
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (JobExecutionException e) {
			fail(StringUtils.stringifyException(e));
		}
	}

	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a successively failing inner vertices. In
	 * particular, the test covers the situation when a vertex fails whose checkpoint is currently used for recovery
	 * itself.
	 */
	@Test
	public void testSuccessivelyFailingInnerVertices() {

		final JobGraph jobGraph = new JobGraph("Job with failing inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex1.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 145613);
		innerVertex1.getConfiguration().setInteger(FAILURE_INDEX_KEY, 2);

		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(InnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex2.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 95490);
		innerVertex2.getConfiguration().setInteger(FAILURE_INDEX_KEY, 0);

		final JobTaskVertex innerVertex3 = new JobTaskVertex("Inner vertex 3", jobGraph);
		innerVertex3.setTaskClass(InnerTask.class);
		innerVertex3.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex3.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		innerVertex2.setVertexToShareInstancesWith(input);
		innerVertex3.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(innerVertex2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex2.connectTo(innerVertex3, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex3.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Reset the FAILED_ONCE flags
		synchronized (FAILED_ONCE) {
			FAILED_ONCE.clear();
		}

		// Create job client and launch job
		try {
			JobClient jobClient = new JobClient(jobGraph, configuration);
			jobClient.submitJobAndWait();
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (JobExecutionException e) {
			fail(StringUtils.stringifyException(e));
		}

	}
	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a successively failing one inner vertices.
	 */
	@Test
	public void testRepeatedlyFailingSameInnerVertex() {

		final JobGraph jobGraph = new JobGraph("Job with repeatedly failing inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		//Using re-failing inner task
		innerVertex1.setTaskClass(RefailingInnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex1.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 145613);
		innerVertex1.getConfiguration().setInteger(FAILURE_INDEX_KEY, 2);

		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(InnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex3 = new JobTaskVertex("Inner vertex 3", jobGraph);
		innerVertex3.setTaskClass(InnerTask.class);
		innerVertex3.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex3.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		innerVertex2.setVertexToShareInstancesWith(input);
		innerVertex3.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(innerVertex2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex2.connectTo(innerVertex3, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex3.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Reset the FAILED_ONCE flags
		synchronized (FAILED_ONCE) {
			FAILED_ONCE.clear();
		}

		// Create job client and launch job
		try {
			JobClient jobClient = new JobClient(jobGraph, configuration);
			jobClient.submitJobAndWait();
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (JobExecutionException e) {
			//This is expected here
			assert(e.isJobCanceledByUser() == false);
			return;
		}
		fail("Job expected to be cancled");
	}
	
	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a successively failing one inner vertices.
	 */
	@Test
	public void testSuccessivelyFailingSeveralInnerVertices() {

		final JobGraph jobGraph = new JobGraph("Job with several successively failing inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex1.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 145613);
		innerVertex1.getConfiguration().setInteger(FAILURE_INDEX_KEY, 2);

		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(InnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex2.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 32563);
		innerVertex2.getConfiguration().setInteger(FAILURE_INDEX_KEY, 1);

		final JobTaskVertex innerVertex3 = new JobTaskVertex("Inner vertex 3", jobGraph);
		innerVertex3.setTaskClass(InnerTask.class);
		innerVertex3.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex3.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex2.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 158563);
		innerVertex2.getConfiguration().setInteger(FAILURE_INDEX_KEY, 0);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		innerVertex2.setVertexToShareInstancesWith(input);
		innerVertex3.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(innerVertex2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex2.connectTo(innerVertex3, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex3.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Reset the FAILED_ONCE flags
		synchronized (FAILED_ONCE) {
			FAILED_ONCE.clear();
		}

		// Create job client and launch job
		try {
			JobClient jobClient = new JobClient(jobGraph, configuration);
			jobClient.submitJobAndWait();
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (JobExecutionException e) {
			fail(StringUtils.stringifyException(e));
		}

	}
	
	@After public void cleanUp(){
		File file = new File("/tmp/");
		File[] files= file.listFiles();

		for (int i = 0; i < files.length; i++) {
			String name = files[i].getName();
			if (name.startsWith("fb") || name.startsWith("checkpoint_")) {
				files[i].delete();
			}
		}
		
		System.out.println("deleted");
		
	   
	}
	
}
