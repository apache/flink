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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.junit.Test;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
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

/**
 * This test is intended to cover the basic functionality of the {@link JobManager}.
 * 
 * @author wenjun
 * @author warneke
 */
public class JobManagerTest {

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
	}

	/**
	 * The content of the input file used during the test.
	 */
	private static final String INPUTCONTENT = "aa\r\nbb\r\ncc\r\ndd\r\nee\r\nff\r\ngg\r\n";

	/**
	 * Writes the input file used during this test to the directory for temporary files.
	 * 
	 * @param inputFilename
	 *        the name of the input file
	 * @return a {@link File} object referring to the input file
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the input data
	 */
	private File createInputFile(String inputFilename) throws IOException {

		final File inputFile = new File(getTempDir() + File.separator + inputFilename);

		if (inputFile.exists()) {
			inputFile.delete();
		}

		inputFile.createNewFile();
		FileWriter fw = new FileWriter(inputFile);
		fw.write(INPUTCONTENT);
		fw.close();

		return inputFile;
	}

	/**
	 * Reads the path to the directory for temporary files from the configuration and returns it.
	 * 
	 * @return the path to the directory for temporary files
	 */
	private String getTempDir() {

		return GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
	}

	/**
	 * Constructs a random filename. The filename is a string of 16 hex characters followed by a <code>.dat</code>
	 * prefix.
	 * 
	 * @return the random filename
	 */
	private String getRandomFilename() {

		final char[] alphabeth = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		String filename = "";
		for (int i = 0; i < 16; i++) {
			filename += alphabeth[(int) (Math.random() * alphabeth.length)];
		}

		return filename + ".dat";
	}

	/**
	 * Creates a jar file from the class with the given class name and stores it in the directory for temporary files.
	 * 
	 * @param className
	 *        the name of the class to create a jar file from
	 * @return a {@link File} object referring to the jar file
	 * @throws IOException
	 *         thrown if an error occurs while writing the jar file
	 */
	private File createJarFile(String className) throws IOException {

		final String jarPath = getTempDir() + File.separator + className + ".jar";
		final File jarFile = new File(jarPath);

		if (jarFile.exists()) {
			jarFile.delete();
		}

		final JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarPath), new Manifest());
		final String classPath = JobManagerTest.class.getResource("").getPath() + className + ".class";
		final File classFile = new File(classPath);

		String packageName = JobManagerTest.class.getPackage().getName();
		packageName = packageName.replaceAll("\\.", "\\/");
		jos.putNextEntry(new JarEntry("/" + packageName + "/" + className + ".class"));

		final FileInputStream fis = new FileInputStream(classFile);
		final byte[] buffer = new byte[1024];
		int num = fis.read(buffer);

		while (num != -1) {
			jos.write(buffer, 0, num);
			num = fis.read(buffer);
		}

		fis.close();
		jos.close();

		return jarFile;
	}

	/**
	 * This test starts Nephele in local mode and executes a simple forwarding task. The test is considered successful
	 * if the input data equals the output data.
	 */
	@Test
	public void test() {

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

		// Create job manager thread and run it
		final JobManagerThread jobManagerThread = new JobManagerThread(jobManager);
		jobManagerThread.start();

		try {

			// Get name of the forward class
			final String forwardClassName = ForwardTask.class.getSimpleName();

			// Create input and jar files
			final String inputFilename = getRandomFilename();
			final File inputFile = createInputFile(inputFilename);
			final File jarFile = createJarFile(forwardClassName);

			// Create job graph
			final JobGraph jg = new JobGraph("Job Graph 1");

			// input vertex
			final JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
			i1.setFileInputClass(FileLineReader.class);
			i1.setFilePath(new Path("file://" + getTempDir() + File.separator + inputFilename));

			// task vertex
			final JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
			t1.setTaskClass(ForwardTask.class);

			// output vertex
			final String outputFilename = getRandomFilename();
			JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
			o1.setFileOutputClass(FileLineWriter.class);
			o1.setFilePath(new Path("file://" + getTempDir() + File.separator + outputFilename));

			// connect vertices
			try {
				i1.connectTo(t1, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
				t1.connectTo(o1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			} catch (JobGraphDefinitionException e) {
				e.printStackTrace();
			}

			// add jar
			jg.addJar(new Path("file://" + getTempDir() + File.separator + forwardClassName + ".jar"));

			// Create job client and launch job
			JobClient jobClient = new JobClient(jg);
			jobClient.submitJobAndWait();

			final char[] buffer = new char[INPUTCONTENT.toCharArray().length];

			// check whether the output file is the same as the input file
			FileReader fr = new FileReader(new File(getTempDir() + File.separator + outputFilename));
			fr.read(buffer);
			fr.close();

			assertEquals(INPUTCONTENT, new String(buffer));

			// Remove temporary files
			inputFile.delete();
			new File(getTempDir() + File.separator + outputFilename).delete();
			jarFile.delete();

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {

			// Stop job manager
			jobManagerThread.interrupt();

			// Wait until the shut down is complete
			while (!jobManager.isShutDown()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException i) {
					break;
				}
			}
		}
	}
}
