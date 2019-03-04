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

package org.apache.flink.container.entrypoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.container.entrypoint.ClassPathJobGraphRetriever.JarsOnClassPath;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link ClassPathJobGraphRetriever}.
 */
public class ClassPathJobGraphRetrieverTest extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final String[] PROGRAM_ARGUMENTS = {"--arg", "suffix"};

	@Test
	public void testJobGraphRetrieval() throws FlinkException {
		final int parallelism = 42;
		final Configuration configuration = new Configuration();
		configuration.setInteger(CoreOptions.DEFAULT_PARALLELISM, parallelism);
		final JobID jobId = new JobID();

		final ClassPathJobGraphRetriever classPathJobGraphRetriever = new ClassPathJobGraphRetriever(
			jobId,
			SavepointRestoreSettings.none(),
			PROGRAM_ARGUMENTS,
			TestJob.class.getCanonicalName());

		final JobGraph jobGraph = classPathJobGraphRetriever.retrieveJobGraph(configuration);

		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
		assertThat(jobGraph.getMaximumParallelism(), is(parallelism));
		assertEquals(jobGraph.getJobID(), jobId);
	}

	@Test
	public void testJobGraphRetrievalFromJar() throws FlinkException, FileNotFoundException {
		final File testJar = TestJob.getTestJobJar();
		final ClassPathJobGraphRetriever classPathJobGraphRetriever = new ClassPathJobGraphRetriever(
			new JobID(),
			SavepointRestoreSettings.none(),
			PROGRAM_ARGUMENTS,
			// No class name specified, but the test JAR "is" on the class path
			null,
			() -> Collections.singleton(testJar));

		final JobGraph jobGraph = classPathJobGraphRetriever.retrieveJobGraph(new Configuration());

		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
	}

	@Test
	public void testJobGraphRetrievalJobClassNameHasPrecedenceOverClassPath() throws FlinkException, FileNotFoundException {
		final File testJar = new File("non-existing");

		final ClassPathJobGraphRetriever classPathJobGraphRetriever = new ClassPathJobGraphRetriever(
			new JobID(),
			SavepointRestoreSettings.none(),
			PROGRAM_ARGUMENTS,
			// Both a class name is specified and a JAR "is" on the class path
			// The class name should have precedence.
			TestJob.class.getCanonicalName(),
			() -> Collections.singleton(testJar));

		final JobGraph jobGraph = classPathJobGraphRetriever.retrieveJobGraph(new Configuration());

		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
	}

	@Test
	public void testSavepointRestoreSettings() throws FlinkException {
		final Configuration configuration = new Configuration();
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath("foobar", true);
		final JobID jobId = new JobID();

		final ClassPathJobGraphRetriever classPathJobGraphRetriever = new ClassPathJobGraphRetriever(
			jobId,
			savepointRestoreSettings,
			PROGRAM_ARGUMENTS,
			TestJob.class.getCanonicalName());

		final JobGraph jobGraph = classPathJobGraphRetriever.retrieveJobGraph(configuration);

		assertThat(jobGraph.getSavepointRestoreSettings(), is(equalTo(savepointRestoreSettings)));
		assertEquals(jobGraph.getJobID(), jobId);
	}

	@Test
	public void testJarFromClassPathSupplierSanityCheck() {
		Iterable<File> jarFiles = JarsOnClassPath.INSTANCE.get();

		// Junit executes this test, so it should be returned as part of JARs on the class path
		assertThat(jarFiles, hasItem(hasProperty("name", containsString("junit"))));
	}

	@Test
	public void testJarFromClassPathSupplier() throws IOException {
		final File file1 = temporaryFolder.newFile();
		final File file2 = temporaryFolder.newFile();
		final File directory = temporaryFolder.newFolder();

		// Mock java.class.path property. The empty strings are important as the shell scripts
		// that prepare the Flink class path often have such entries.
		final String classPath = javaClassPath(
			"",
			"",
			"",
			file1.getAbsolutePath(),
			"",
			directory.getAbsolutePath(),
			"",
			file2.getAbsolutePath(),
			"",
			"");

		Iterable<File> jarFiles = setClassPathAndGetJarsOnClassPath(classPath);

		assertThat(jarFiles, contains(file1, file2));
	}

	private static String javaClassPath(String... entries) {
		String pathSeparator = System.getProperty(JarsOnClassPath.PATH_SEPARATOR);
		return String.join(pathSeparator, entries);
	}

	private static Iterable<File> setClassPathAndGetJarsOnClassPath(String classPath) {
		final String originalClassPath = System.getProperty(JarsOnClassPath.JAVA_CLASS_PATH);
		try {
			System.setProperty(JarsOnClassPath.JAVA_CLASS_PATH, classPath);
			return JarsOnClassPath.INSTANCE.get();
		} finally {
			// Reset property
			System.setProperty(JarsOnClassPath.JAVA_CLASS_PATH, originalClassPath);
		}
	}

}
