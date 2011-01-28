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

package eu.stratosphere.nephele.jobmanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.library.FileLineReader;
import eu.stratosphere.nephele.io.library.FileLineWriter;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.util.ServerTestUtils;

/**
 * This test is intended to cover the basic functionality of the {@link JobManager}.
 * 
 * @author wenjun
 * @author warneke
 */
public class JobManagerTest {

	private static JobManagerThread jobManagerThread = null;

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
		private JobManagerThread(JobManager jobManager) {

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

			// create the job manager
			JobManager jobManager = null;

			try {

				Constructor<JobManager> c = JobManager.class.getDeclaredConstructor(new Class[] { String.class,
					String.class });
				c.setAccessible(true);
				jobManager = c.newInstance(new Object[] { new String(System.getProperty("user.dir") + "/correct-conf"),
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

	/**
	 * Tests of the Nephele channels with a large (> 1 MB) file.
	 */
	@Test
	public void testExecutionWithLargeInputFile() {
		test(1000000);
	}

	/**
	 * Tests of the Nephele channels with a file of zero bytes size.
	 */
	@Test
	public void testExecutionWithZeroSizeInputFile() {
		test(0);
	}

	/**
	 * Tests the Nephele execution when an exception occurs. In particular, it is tested if the information that is
	 * wrapped by the exception is correctly passed on to the client.
	 */
	@Test
	public void testExecutionWithException() {

		final String exceptionClassName = ExceptionTask.class.getSimpleName();
		File inputFile = null;
		File outputFile = null;
		File jarFile = null;

		try {

			inputFile = ServerTestUtils.createInputFile(0);
			outputFile = new File(ServerTestUtils.getTempDir() + File.separator
				+ ServerTestUtils.getRandomFilename());
			jarFile = ServerTestUtils.createJarFile(exceptionClassName);

			// Create job graph
			final JobGraph jg = new JobGraph("Job Graph for Exception Test");

			// input vertex
			final JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
			i1.setFileInputClass(FileLineReader.class);
			i1.setFilePath(new Path("file://" + inputFile.getAbsolutePath().toString()));

			// task vertex 1
			final JobTaskVertex t1 = new JobTaskVertex("Task with Exception", jg);
			t1.setTaskClass(ExceptionTask.class);

			// output vertex
			JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
			o1.setFileOutputClass(FileLineWriter.class);
			o1.setFilePath(new Path("file://" + outputFile.getAbsolutePath().toString()));

			t1.setVertexToShareInstancesWith(i1);
			o1.setVertexToShareInstancesWith(i1);

			// connect vertices
			i1.connectTo(t1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			t1.connectTo(o1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

			// add jar
			jg.addJar(new Path("file://" + ServerTestUtils.getTempDir() + File.separator + exceptionClassName + ".jar"));

			// Create job client and launch job
			final JobClient jobClient = new JobClient(jg);

			try {
				jobClient.submitJobAndWait();
			} catch (JobExecutionException e) {
				// Check if the correct error message is encapsulated in the exception
				if (e.getMessage() == null) {
					fail("JobExecutionException does not contain an error message");
				}
				if (!e.getMessage().contains(ExceptionTask.ERROR_MESSAGE)) {
					fail("JobExecutionException does not contain the expected error message");
				}

				return;
			}

			fail("Expected exception but did not receive it");

		} catch (JobGraphDefinitionException jgde) {
			fail(jgde.getMessage());
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {

			// Remove temporary files
			if (inputFile != null) {
				inputFile.delete();
			}
			if (outputFile != null) {
				outputFile.delete();
			}
			if (jarFile != null) {
				jarFile.delete();
			}
		}
	}

	/**
	 * Creates a file with a sequence of 0 to <code>limit</code> integer numbers
	 * and triggers a sample job. The sample reads all the numbers from the input file and pushes them through a
	 * network, a file, and an in-memory channel. Eventually, the numbers are written back to an output file. The test
	 * is considered successful if the input file equals the output file.
	 * 
	 * @param limit
	 *        the upper bound for the sequence of numbers to be generated
	 */
	private void test(int limit) {

		try {

			// Get name of the forward class
			final String forwardClassName = ForwardTask.class.getSimpleName();

			// Create input and jar files
			final File inputFile = ServerTestUtils.createInputFile(limit);
			final File outputFile = new File(ServerTestUtils.getTempDir() + File.separator
				+ ServerTestUtils.getRandomFilename());
			final File jarFile = ServerTestUtils.createJarFile(forwardClassName);

			// Create job graph
			final JobGraph jg = new JobGraph("Job Graph 1");

			// input vertex
			final JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
			i1.setFileInputClass(FileLineReader.class);
			i1.setFilePath(new Path("file://" + inputFile.getAbsolutePath().toString()));

			// task vertex 1
			final JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
			t1.setTaskClass(ForwardTask.class);

			// task vertex 2
			final JobTaskVertex t2 = new JobTaskVertex("Task 2", jg);
			t2.setTaskClass(ForwardTask.class);

			// output vertex
			JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
			o1.setFileOutputClass(FileLineWriter.class);
			o1.setFilePath(new Path("file://" + outputFile.getAbsolutePath().toString()));

			t1.setVertexToShareInstancesWith(i1);
			t2.setVertexToShareInstancesWith(i1);
			o1.setVertexToShareInstancesWith(i1);

			// connect vertices
			try {
				i1.connectTo(t1, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
				t1.connectTo(t2, ChannelType.FILE, CompressionLevel.NO_COMPRESSION);
				t2.connectTo(o1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			} catch (JobGraphDefinitionException e) {
				e.printStackTrace();
			}

			// add jar
			jg.addJar(new Path("file://" + ServerTestUtils.getTempDir() + File.separator + forwardClassName + ".jar"));

			// Create job client and launch job
			JobClient jobClient = new JobClient(jg);
			try {
				jobClient.submitJobAndWait();
			} catch (JobExecutionException e) {
				fail(e.getMessage());
			}

			// Finally, compare output file to initial number sequence
			final BufferedReader bufferedReader = new BufferedReader(new FileReader(outputFile));
			for (int i = 0; i < limit; i++) {
				final String number = bufferedReader.readLine();
				try {
					assertEquals(i, Integer.parseInt(number));
				} catch (NumberFormatException e) {
					fail(e.getMessage());
				}
			}

			bufferedReader.close();

			// Remove temporary files
			inputFile.delete();
			outputFile.delete();
			jarFile.delete();

		} catch (IOException ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}
	}
}
