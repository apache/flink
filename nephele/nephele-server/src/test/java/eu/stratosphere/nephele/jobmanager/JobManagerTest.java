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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.library.FileLineReader;
import eu.stratosphere.nephele.io.library.FileLineWriter;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobmanager.JobManager;

public class JobManagerTest {

	private static final String PATHNAME = "/tmp/";

	private static final String INPUTCONTENT = "aa\r\nbb\r\ncc\r\ndd\r\nee\r\nff\r\ngg\r\n";

	private static final int SLEEPINTERVAL = 5000;

	private void createInputFile(String inputFilename) {

		File inputFile = new File(PATHNAME + inputFilename);

		if (inputFile.exists()) {
			inputFile.delete();
		}

		try {
			inputFile.createNewFile();
			FileWriter fw = new FileWriter(inputFile);
			fw.write(INPUTCONTENT);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String getRandomFilename() {

		final char[] alphabeth = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		String filename = "";
		for (int i = 0; i < 16; i++) {
			filename += alphabeth[(int) Math.random() * alphabeth.length];
		}

		return filename + ".dat";
	}

	private void createJarFile(String className) {

		String jarPath = PATHNAME + className + ".jar";
		File jarFile = new File(jarPath);

		if (jarFile.exists()) {
			jarFile.delete();
		}

		try {
			JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarPath), new Manifest());
			String classPath = JobManagerTest.class.getResource("").getPath() + className + ".class";
			File classFile = new File(classPath);

			String packageName = JobManagerTest.class.getPackage().getName();
			packageName = packageName.replaceAll("\\.", "\\/");
			jos.putNextEntry(new JarEntry("/" + packageName + "/" + className + ".class"));

			FileInputStream fis = new FileInputStream(classFile);
			byte[] buffer = new byte[1024];
			int num = fis.read(buffer);

			while (num != -1) {
				jos.write(buffer, 0, num);
				num = fis.read(buffer);
			}

			fis.close();
			jos.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

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
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		try {
			Thread.sleep(SLEEPINTERVAL);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// create input and jar files
		final String inputFilename = getRandomFilename();
		createInputFile(inputFilename);
		createJarFile("GrepTask");

		// create job graph
		JobGraph jg = new JobGraph("Job Graph 1");
		JobID jobID = jg.getJobID();

		// input vertex
		JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
		i1.setFileInputClass(FileLineReader.class);
		i1.setFilePath(new Path("file://" + PATHNAME + inputFilename));

		// task vertex
		JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
		t1.setTaskClass(GrepTask.class);

		// output vertex
		final String outputFilename = getRandomFilename();
		JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
		o1.setFileOutputClass(FileLineWriter.class);
		o1.setFilePath(new Path("file://" + PATHNAME + outputFilename));

		// connect vertices
		try {
			i1.connectTo(t1, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
			t1.connectTo(o1, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
		}

		// add jar
		jg.addJar(new Path("file://" + PATHNAME + "GrepTask.jar"));
		Path[] paths = { new Path("file://" + PATHNAME + "GrepTask.jar") };

		try {
			LibraryCacheManager.addLibrary(jobID, new Path("file://" + PATHNAME + "GrepTask.jar"), new File(PATHNAME
				+ "GrepTask.jar").length(), new DataInputStream(
				new FileInputStream(new File(PATHNAME + "GrepTask.jar"))));
			LibraryCacheManager.register(jobID, paths);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// submit the job
		try {
			jobManager.submitJob(jg);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// in the test, we trigger the
		jobManager.runVerticesReadyForExecution();

		try {
			Thread.sleep(SLEEPINTERVAL);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		char[] buffer = new char[INPUTCONTENT.toCharArray().length];

		// check whether the output file is the same as the input file
		try {
			FileReader fr = new FileReader(new File(PATHNAME + outputFilename));
			fr.read(buffer);
			fr.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		assertEquals(INPUTCONTENT, new String(buffer));

		// Remove temporary files
		new File(PATHNAME + inputFilename).delete();
		new File(PATHNAME + outputFilename).delete();

		// Shutdown the job manager
		jobManager.cleanUp();

		// Wait for the job manager to be shut down
		try {
			Thread.sleep(SLEEPINTERVAL);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
