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

package org.apache.flink.client.deployment.application;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.testjar.TestJob;
import org.apache.flink.client.testjar.TestJobInfo;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link ClassPathPackagedProgramRetriever}.
 */
public class ClassPathPackagedProgramRetrieverTest extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final TemporaryFolder JOB_DIRS = new TemporaryFolder();

	private static final String[] PROGRAM_ARGUMENTS = {"--arg", "suffix"};

	/*
	 * The directory structure used to test
	 *
	 * userDirHasEntryClass/
	 *                    |_jarWithEntryClass
	 *                    |_jarWithoutEntryClass
	 *                    |_textFile
	 *
	 * userDirHasNotEntryClass/
	 *                       |_jarWithoutEntryClass
	 *                       |_textFile
	 */

	private static final Collection<URL> expectedURLs = new ArrayList<>();

	private static File userDirHasEntryClass;

	private static File userDirHasNotEntryClass;

	@BeforeClass
	public static void init() throws IOException {
		final String textFileName = "test.txt";
		final String userDirHasEntryClassName = "_test_user_dir_has_entry_class";
		final String userDirHasNotEntryClassName = "_test_user_dir_has_not_entry_class";

		userDirHasEntryClass = JOB_DIRS.newFolder(userDirHasEntryClassName);
		final Path userJarPath = userDirHasEntryClass.toPath().resolve(TestJobInfo.JOB_JAR_PATH.toFile().getName());
		final Path userLibJarPath =
			userDirHasEntryClass.toPath().resolve(TestJobInfo.JOB_LIB_JAR_PATH.toFile().getName());
		userDirHasNotEntryClass = JOB_DIRS.newFolder(userDirHasNotEntryClassName);

		//create files
		Files.copy(TestJobInfo.JOB_JAR_PATH, userJarPath);
		Files.copy(TestJobInfo.JOB_LIB_JAR_PATH, userLibJarPath);
		Files.createFile(userDirHasEntryClass.toPath().resolve(textFileName));

		Files.copy(TestJobInfo.JOB_LIB_JAR_PATH, userDirHasNotEntryClass.toPath().resolve(TestJobInfo.JOB_LIB_JAR_PATH.toFile().getName()));
		Files.createFile(userDirHasNotEntryClass.toPath().resolve(textFileName));

		final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
		Arrays.asList(userJarPath, userLibJarPath)
			.stream()
			.map(path -> FileUtils.relativizePath(workingDirectory, path))
			.map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
			.forEach(expectedURLs::add);
	}

	@Test
	public void testJobGraphRetrieval() throws IOException, FlinkException, ProgramInvocationException {
		final int parallelism = 42;
		final JobID jobId = new JobID();

		final Configuration configuration = new Configuration();
		configuration.setInteger(CoreOptions.DEFAULT_PARALLELISM, parallelism);
		configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());

		final ClassPathPackagedProgramRetriever retrieverUnderTest =
			ClassPathPackagedProgramRetriever.newBuilder(PROGRAM_ARGUMENTS)
				.setJobClassName(TestJob.class.getCanonicalName())
				.build();

		final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, configuration);

		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
		assertThat(jobGraph.getSavepointRestoreSettings(), is(equalTo(SavepointRestoreSettings.none())));
		assertThat(jobGraph.getMaximumParallelism(), is(parallelism));
		assertEquals(jobGraph.getJobID(), jobId);
	}

	@Test
	public void testJobGraphRetrievalFromJar() throws IOException, FlinkException, ProgramInvocationException {
		final File testJar = TestJob.getTestJobJar();
		final ClassPathPackagedProgramRetriever retrieverUnderTest =
			ClassPathPackagedProgramRetriever.newBuilder(PROGRAM_ARGUMENTS)
				.setJarsOnClassPath(() -> Collections.singleton(testJar))
				.build();

		final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
	}

	@Test
	public void testJobGraphRetrievalJobClassNameHasPrecedenceOverClassPath() throws IOException, FlinkException, ProgramInvocationException {
		final File testJar = new File("non-existing");

		final ClassPathPackagedProgramRetriever retrieverUnderTest =
			ClassPathPackagedProgramRetriever.newBuilder(PROGRAM_ARGUMENTS)
				// Both a class name is specified and a JAR "is" on the class path
				// The class name should have precedence.
			.setJobClassName(TestJob.class.getCanonicalName())
			.setJarsOnClassPath(() -> Collections.singleton(testJar))
			.build();

		final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

		assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
	}

	@Test
	public void testSavepointRestoreSettings() throws FlinkException, IOException, ProgramInvocationException {
		final Configuration configuration = new Configuration();
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath("foobar", true);
		final JobID jobId = new JobID();

		configuration.setString(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
		SavepointRestoreSettings.toConfiguration(savepointRestoreSettings, configuration);

		final ClassPathPackagedProgramRetriever retrieverUnderTest =
			ClassPathPackagedProgramRetriever.newBuilder(PROGRAM_ARGUMENTS)
			.setJobClassName(TestJob.class.getCanonicalName())
			.build();

		final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, configuration);

		assertThat(jobGraph.getSavepointRestoreSettings(), is(equalTo(savepointRestoreSettings)));
		assertEquals(jobGraph.getJobID(), jobId);
	}

	@Test
	public void testJarFromClassPathSupplierSanityCheck() {
		Iterable<File> jarFiles = ClassPathPackagedProgramRetriever.JarsOnClassPath.INSTANCE.get();

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

	@Test
	public void testJobGraphRetrievalFailIfJobDirDoesNotHaveEntryClass() throws IOException, ProgramInvocationException {
		final File testJar = TestJob.getTestJobJar();
		final ClassPathPackagedProgramRetriever retrieverUnderTest =
			ClassPathPackagedProgramRetriever.newBuilder(PROGRAM_ARGUMENTS)
				.setJarsOnClassPath(() -> Collections.singleton(testJar))
				.setUserLibDirectory(userDirHasNotEntryClass)
				.build();
		try {
			retrieveJobGraph(retrieverUnderTest, new Configuration());
			Assert.fail("This case should throw exception !");
		} catch (FlinkException e) {
			assertTrue(ExceptionUtils
				.findThrowableWithMessage(e, "Failed to find job JAR on class path")
				.isPresent());
		}
	}

	@Test
	public void testJobGraphRetrievalFailIfDoesNotFindTheEntryClassInTheJobDir() throws IOException, ProgramInvocationException {
		final ClassPathPackagedProgramRetriever retrieverUnderTest =
			ClassPathPackagedProgramRetriever.newBuilder(PROGRAM_ARGUMENTS)
				.setJobClassName(TestJobInfo.JOB_CLASS)
				.setJarsOnClassPath(Collections::emptyList)
				.setUserLibDirectory(userDirHasNotEntryClass)
				.build();
		try {
			retrieveJobGraph(retrieverUnderTest, new Configuration());
			Assert.fail("This case should throw class not found exception!!");
		} catch (FlinkException e) {
			assertTrue(ExceptionUtils
				.findThrowableWithMessage(e, "Could not find the provided job class")
				.isPresent());
		}

	}

	@Test
	public void testRetrieveCorrectUserClasspathsWithoutSpecifiedEntryClass() throws IOException, FlinkException, ProgramInvocationException {
		final ClassPathPackagedProgramRetriever retrieverUnderTest =
			ClassPathPackagedProgramRetriever.newBuilder(PROGRAM_ARGUMENTS)
				.setJarsOnClassPath(Collections::emptyList)
				.setUserLibDirectory(userDirHasEntryClass)
				.build();
		final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

		assertThat(
				jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList()),
				containsInAnyOrder(expectedURLs.stream().map(URL::toString).toArray()));
	}

	@Test
	public void testRetrieveCorrectUserClasspathsWithSpecifiedEntryClass() throws IOException, FlinkException, ProgramInvocationException {
		final ClassPathPackagedProgramRetriever retrieverUnderTest =
			ClassPathPackagedProgramRetriever.newBuilder(PROGRAM_ARGUMENTS)
				.setJobClassName(TestJobInfo.JOB_CLASS)
				.setJarsOnClassPath(Collections::emptyList)
				.setUserLibDirectory(userDirHasEntryClass)
				.build();
		final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

		assertThat(
				jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList()),
				containsInAnyOrder(expectedURLs.stream().map(URL::toString).toArray()));
	}

	@Test
	public void testRetrieveFromJarFileWithoutUserLib() throws IOException, FlinkException, ProgramInvocationException {
		final File testJar = TestJob.getTestJobJar();
		final ClassPathPackagedProgramRetriever retrieverUnderTest =
			ClassPathPackagedProgramRetriever.newBuilder(PROGRAM_ARGUMENTS)
				.setJarFile(testJar)
				.build();
		final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

		assertThat(jobGraph.getUserJars(), containsInAnyOrder(new org.apache.flink.core.fs.Path(testJar.toURI())));
		assertThat(jobGraph.getClasspaths().isEmpty(), is(true));
	}

	@Test
	public void testRetrieveFromJarFileWithUserLib() throws IOException, FlinkException, ProgramInvocationException {
		final File testJar = TestJob.getTestJobJar();
		final ClassPathPackagedProgramRetriever retrieverUnderTest =
			ClassPathPackagedProgramRetriever.newBuilder(PROGRAM_ARGUMENTS)
				.setJarFile(testJar)
				.setUserLibDirectory(userDirHasEntryClass)
				.build();
		final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

		assertThat(jobGraph.getUserJars(), containsInAnyOrder(new org.apache.flink.core.fs.Path(testJar.toURI())));
		assertThat(
			jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList()),
			containsInAnyOrder(expectedURLs.stream().map(URL::toString).toArray()));
	}

	private JobGraph retrieveJobGraph(ClassPathPackagedProgramRetriever retrieverUnderTest, Configuration configuration) throws FlinkException, ProgramInvocationException {
		final PackagedProgram packagedProgram = retrieverUnderTest.getPackagedProgram();

		final int defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, packagedProgram.getJobJarAndDependencies(), URL::toString);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.CLASSPATHS, packagedProgram.getClasspaths(), URL::toString);

		final Pipeline pipeline = PackagedProgramUtils
				.getPipelineFromProgram(packagedProgram, configuration, defaultParallelism, false);
		return PipelineExecutorUtils.getJobGraph(pipeline, configuration);
	}

	private static String javaClassPath(String... entries) {
		String pathSeparator = System.getProperty(ClassPathPackagedProgramRetriever.JarsOnClassPath.PATH_SEPARATOR);
		return String.join(pathSeparator, entries);
	}

	private static Iterable<File> setClassPathAndGetJarsOnClassPath(String classPath) {
		final String originalClassPath = System.getProperty(ClassPathPackagedProgramRetriever.JarsOnClassPath.JAVA_CLASS_PATH);
		try {
			System.setProperty(ClassPathPackagedProgramRetriever.JarsOnClassPath.JAVA_CLASS_PATH, classPath);
			return ClassPathPackagedProgramRetriever.JarsOnClassPath.INSTANCE.get();
		} finally {
			// Reset property
			System.setProperty(ClassPathPackagedProgramRetriever.JarsOnClassPath.JAVA_CLASS_PATH, originalClassPath);
		}
	}
}
