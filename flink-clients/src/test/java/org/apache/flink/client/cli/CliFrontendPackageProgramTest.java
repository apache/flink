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

package org.apache.flink.client.cli;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.TestLogger;

import org.apache.commons.cli.CommandLine;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Collections;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_CLASSLOADERTEST_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getNonJarFilePath;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for the RUN command with {@link PackagedProgram PackagedPrograms}.
 */
public class CliFrontendPackageProgramTest extends TestLogger {

	private CliFrontend frontend;

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@AfterClass
	public static void shutdown() {
		CliFrontendTestUtils.restoreSystemOut();
	}

	@Before
	public void setup() throws Exception {
		final Configuration configuration = new Configuration();
		frontend = new CliFrontend(
			configuration,
			Collections.singletonList(new DefaultCLI()));
	}

	@Test
	public void testNonExistingJarFile() throws Exception {
		ProgramOptions programOptions = mock(ProgramOptions.class);
		when(programOptions.getJarFilePath()).thenReturn("/some/none/existing/path");

		try {
			frontend.buildProgram(programOptions);
			fail("should throw an exception");
		}
		catch (FileNotFoundException e) {
			// that's what we want
		}
	}

	@Test
	public void testFileNotJarFile() throws Exception {
		ProgramOptions programOptions = mock(ProgramOptions.class);
		when(programOptions.getJarFilePath()).thenReturn(getNonJarFilePath());
		when(programOptions.getProgramArgs()).thenReturn(new String[0]);
		when(programOptions.getSavepointRestoreSettings()).thenReturn(SavepointRestoreSettings.none());

		try {
			frontend.buildProgram(programOptions);
			fail("should throw an exception");
		} catch (ProgramInvocationException e) {
			// that's what we want
		}
	}

	@Test
	public void testVariantWithExplicitJarAndArgumentsOption() throws Exception {
		String[] arguments = {
				"--classpath", "file:///tmp/foo",
				"--classpath", "file:///tmp/bar",
				"-j", getTestJarPath(),
				"-a", "--debug", "true", "arg1", "arg2" };
		URL[] classpath = new URL[] { new URL("file:///tmp/foo"), new URL("file:///tmp/bar") };
		String[] reducedArguments = new String[] {"--debug", "true", "arg1", "arg2"};

		CommandLine commandLine = CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
		ProgramOptions programOptions = ProgramOptions.create(commandLine);

		assertEquals(getTestJarPath(), programOptions.getJarFilePath());
		assertArrayEquals(classpath, programOptions.getClasspaths().toArray());
		assertArrayEquals(reducedArguments, programOptions.getProgramArgs());

		PackagedProgram prog = frontend.buildProgram(programOptions);

		Assert.assertArrayEquals(reducedArguments, prog.getArguments());
		Assert.assertEquals(TEST_JAR_MAIN_CLASS, prog.getMainClassName());
	}

	@Test
	public void testVariantWithExplicitJarAndNoArgumentsOption() throws Exception {
		String[] arguments = {
				"--classpath", "file:///tmp/foo",
				"--classpath", "file:///tmp/bar",
				"-j", getTestJarPath(),
				"--debug", "true", "arg1", "arg2" };
		URL[] classpath = new URL[] { new URL("file:///tmp/foo"), new URL("file:///tmp/bar") };
		String[] reducedArguments = new String[] {"--debug", "true", "arg1", "arg2"};

		CommandLine commandLine = CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
		ProgramOptions programOptions = ProgramOptions.create(commandLine);

		assertEquals(getTestJarPath(), programOptions.getJarFilePath());
		assertArrayEquals(classpath, programOptions.getClasspaths().toArray());
		assertArrayEquals(reducedArguments, programOptions.getProgramArgs());

		PackagedProgram prog = frontend.buildProgram(programOptions);

		Assert.assertArrayEquals(reducedArguments, prog.getArguments());
		Assert.assertEquals(TEST_JAR_MAIN_CLASS, prog.getMainClassName());
	}

	@Test
	public void testValidVariantWithNoJarAndNoArgumentsOption() throws Exception {
		String[] arguments = {
				"--classpath", "file:///tmp/foo",
				"--classpath", "file:///tmp/bar",
				getTestJarPath(),
				"--debug", "true", "arg1", "arg2" };
		URL[] classpath = new URL[] { new URL("file:///tmp/foo"), new URL("file:///tmp/bar") };
		String[] reducedArguments = {"--debug", "true", "arg1", "arg2"};

		CommandLine commandLine = CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
		ProgramOptions programOptions = ProgramOptions.create(commandLine);

		assertEquals(getTestJarPath(), programOptions.getJarFilePath());
		assertArrayEquals(classpath, programOptions.getClasspaths().toArray());
		assertArrayEquals(reducedArguments, programOptions.getProgramArgs());

		PackagedProgram prog = frontend.buildProgram(programOptions);

		Assert.assertArrayEquals(reducedArguments, prog.getArguments());
		Assert.assertEquals(TEST_JAR_MAIN_CLASS, prog.getMainClassName());
	}

	@Test(expected = CliArgsException.class)
	public void testNoJarNoArgumentsAtAll() throws Exception {
		frontend.run(new String[0]);
	}

	@Test
	public void testNonExistingFileWithArguments() throws Exception {
		String[] arguments = {
				"--classpath", "file:///tmp/foo",
				"--classpath", "file:///tmp/bar",
				"/some/none/existing/path",
				"--debug", "true", "arg1", "arg2"  };
		URL[] classpath = new URL[] { new URL("file:///tmp/foo"), new URL("file:///tmp/bar") };
		String[] reducedArguments = {"--debug", "true", "arg1", "arg2"};

		CommandLine commandLine = CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
		ProgramOptions programOptions = ProgramOptions.create(commandLine);

		assertEquals(arguments[4], programOptions.getJarFilePath());
		assertArrayEquals(classpath, programOptions.getClasspaths().toArray());
		assertArrayEquals(reducedArguments, programOptions.getProgramArgs());

		try {
			frontend.buildProgram(programOptions);
			fail("Should fail with an exception");
		}
		catch (FileNotFoundException e) {
			// that's what we want
		}
	}

	@Test
	public void testNonExistingFileWithoutArguments() throws Exception {
		String[] arguments = {"/some/none/existing/path"};

		CommandLine commandLine = CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
		ProgramOptions programOptions = ProgramOptions.create(commandLine);

		assertEquals(arguments[0], programOptions.getJarFilePath());
		assertArrayEquals(new String[0], programOptions.getProgramArgs());

		try {
			frontend.buildProgram(programOptions);
		}
		catch (FileNotFoundException e) {
			// that's what we want
		}
	}

	/**
	 * Ensure that we will never have the following error.
	 *
	 * <pre>
	 * 	org.apache.flink.client.program.ProgramInvocationException: The main method caused an error.
	 *		at org.apache.flink.client.program.PackagedProgram.callMainMethod(PackagedProgram.java:398)
	 *		at org.apache.flink.client.program.PackagedProgram.invokeInteractiveModeForExecution(PackagedProgram.java:301)
	 *		at org.apache.flink.client.program.Client.getOptimizedPlan(Client.java:140)
	 *		at org.apache.flink.client.program.Client.getOptimizedPlanAsJson(Client.java:125)
	 *		at org.apache.flink.client.cli.CliFrontend.info(CliFrontend.java:439)
	 *		at org.apache.flink.client.cli.CliFrontend.parseParameters(CliFrontend.java:931)
	 *		at org.apache.flink.client.cli.CliFrontend.main(CliFrontend.java:951)
	 *	Caused by: java.io.IOException: java.lang.RuntimeException: java.lang.ClassNotFoundException: org.apache.hadoop.hive.ql.io.RCFileInputFormat
	 *		at org.apache.hcatalog.mapreduce.HCatInputFormat.setInput(HCatInputFormat.java:102)
	 *		at org.apache.hcatalog.mapreduce.HCatInputFormat.setInput(HCatInputFormat.java:54)
	 *		at tlabs.CDR_In_Report.createHCatInputFormat(CDR_In_Report.java:322)
	 *		at tlabs.CDR_Out_Report.main(CDR_Out_Report.java:380)
	 *		at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	 *		at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	 *		at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	 *		at java.lang.reflect.Method.invoke(Method.java:622)
	 *		at org.apache.flink.client.program.PackagedProgram.callMainMethod(PackagedProgram.java:383)
	 * </pre>
	 *
	 * <p>The test works as follows:
	 *
	 * <ul>
	 *   <li> Use the CliFrontend to invoke a jar file that loads a class which is only available
	 * 	      in the jarfile itself (via a custom classloader)
	 *   <li> Change the Usercode classloader of the PackagedProgram to a special classloader for this test
	 *   <li> the classloader will accept the special class (and return a String.class)
	 * </ul>
	 */
	@Test
	public void testPlanWithExternalClass() throws Exception {
		final boolean[] callme = { false }; // create a final object reference, to be able to change its val later

		try {
			String[] arguments = {
					"--classpath", "file:///tmp/foo",
					"--classpath", "file:///tmp/bar",
					"-c", TEST_JAR_CLASSLOADERTEST_CLASS, getTestJarPath(),
					"true", "arg1", "arg2" };
			URL[] classpath = new URL[] { new URL("file:///tmp/foo"), new URL("file:///tmp/bar") };
			String[] reducedArguments = { "true", "arg1", "arg2" };

			CommandLine commandLine = CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
			ProgramOptions programOptions = ProgramOptions.create(commandLine);

			assertEquals(getTestJarPath(), programOptions.getJarFilePath());
			assertArrayEquals(classpath, programOptions.getClasspaths().toArray());
			assertEquals(TEST_JAR_CLASSLOADERTEST_CLASS, programOptions.getEntryPointClassName());
			assertArrayEquals(reducedArguments, programOptions.getProgramArgs());

			PackagedProgram prog = spy(frontend.buildProgram(programOptions));

			ClassLoader testClassLoader = new ClassLoader(prog.getUserCodeClassLoader()) {
				@Override
				public Class<?> loadClass(String name) throws ClassNotFoundException {
					if ("org.apache.hadoop.hive.ql.io.RCFileInputFormat".equals(name)) {
						callme[0] = true;
						return String.class; // Intentionally return the wrong class.
					} else {
						return super.loadClass(name);
					}
				}
			};
			when(prog.getUserCodeClassLoader()).thenReturn(testClassLoader);

			assertEquals(TEST_JAR_CLASSLOADERTEST_CLASS, prog.getMainClassName());
			assertArrayEquals(reducedArguments, prog.getArguments());

			Configuration c = new Configuration();
			Optimizer compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), c);

			// we expect this to fail with a "ClassNotFoundException"
			Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(prog, c, 666, true);
			FlinkPipelineTranslationUtil.translateToJSONExecutionPlan(pipeline);
			fail("Should have failed with a ClassNotFoundException");
		}
		catch (ProgramInvocationException e) {
			if (!(e.getCause() instanceof ClassNotFoundException)) {
				e.printStackTrace();
				fail("Program didn't throw ClassNotFoundException");
			}
			assertTrue("Classloader was not called", callme[0]);
		}
	}
}
