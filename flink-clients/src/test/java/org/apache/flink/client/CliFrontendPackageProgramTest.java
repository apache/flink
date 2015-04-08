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

package org.apache.flink.client;

import static org.apache.flink.client.CliFrontendTestUtils.TEST_JAR_CLASSLOADERTEST_CLASS;
import static org.apache.flink.client.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.CliFrontendTestUtils.getNonJarFilePath;
import static org.apache.flink.client.CliFrontendTestUtils.getTestJarPath;
import static org.apache.flink.client.CliFrontendTestUtils.pipeSystemOutToNull;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.net.URL;


public class CliFrontendPackageProgramTest {
	
	@BeforeClass
	public static void init() {
		pipeSystemOutToNull();
	}

	@Test
	public void testNonExistingJarFile() {
		try {
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());

			ProgramOptions options = mock(ProgramOptions.class);
			when(options.getJarFilePath()).thenReturn("/some/none/existing/path");

			try {
				frontend.buildProgram(options);
				fail("should throw an exception");
			}
			catch (FileNotFoundException e) {
				// that's what we want
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testFileNotJarFile() {
		try {
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());

			ProgramOptions options = mock(ProgramOptions.class);
			when(options.getJarFilePath()).thenReturn(getNonJarFilePath());

			try {
				frontend.buildProgram(options);
				fail("should throw an exception");
			}
			catch (ProgramInvocationException e) {
				// that's what we want
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testVariantWithExplicitJarAndArgumentsOption() {
		try {
			String[] arguments = {
					"--classpath", "file:///tmp/foo",
					"--classpath", "file:///tmp/bar",
					"-j", getTestJarPath(),
					"-a", "--debug", "true", "arg1", "arg2" };
			URL[] classpath = new URL[] { new URL("file:///tmp/foo"), new URL("file:///tmp/bar") };
			String[] reducedArguments = new String[] {"--debug", "true", "arg1", "arg2"};

			RunOptions options = CliFrontendParser.parseRunCommand(arguments);
			assertEquals(getTestJarPath(), options.getJarFilePath());
			assertArrayEquals(classpath, options.getClasspaths().toArray());
			assertArrayEquals(reducedArguments, options.getProgramArgs());

			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			PackagedProgram prog = frontend.buildProgram(options);

			Assert.assertArrayEquals(reducedArguments, prog.getArguments());
			Assert.assertEquals(TEST_JAR_MAIN_CLASS, prog.getMainClassName());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testVariantWithExplicitJarAndNoArgumentsOption() {
		try {
			String[] arguments = {
					"--classpath", "file:///tmp/foo",
					"--classpath", "file:///tmp/bar",
					"-j", getTestJarPath(),
					"--debug", "true", "arg1", "arg2" };
			URL[] classpath = new URL[] { new URL("file:///tmp/foo"), new URL("file:///tmp/bar") };
			String[] reducedArguments = new String[] {"--debug", "true", "arg1", "arg2"};

			RunOptions options = CliFrontendParser.parseRunCommand(arguments);
			assertEquals(getTestJarPath(), options.getJarFilePath());
			assertArrayEquals(classpath, options.getClasspaths().toArray());
			assertArrayEquals(reducedArguments, options.getProgramArgs());

			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());

			PackagedProgram prog = frontend.buildProgram(options);

			Assert.assertArrayEquals(reducedArguments, prog.getArguments());
			Assert.assertEquals(TEST_JAR_MAIN_CLASS, prog.getMainClassName());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testValidVariantWithNoJarAndNoArgumentsOption() {
		try {
			String[] arguments = {
					"--classpath", "file:///tmp/foo",
					"--classpath", "file:///tmp/bar",
					getTestJarPath(),
					"--debug", "true", "arg1", "arg2" };
			URL[] classpath = new URL[] { new URL("file:///tmp/foo"), new URL("file:///tmp/bar") };
			String[] reducedArguments = {"--debug", "true", "arg1", "arg2"};

			RunOptions options = CliFrontendParser.parseRunCommand(arguments);
			assertEquals(getTestJarPath(), options.getJarFilePath());
			assertArrayEquals(classpath, options.getClasspaths().toArray());
			assertArrayEquals(reducedArguments, options.getProgramArgs());

			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());

			PackagedProgram prog = frontend.buildProgram(options);

			Assert.assertArrayEquals(reducedArguments, prog.getArguments());
			Assert.assertEquals(TEST_JAR_MAIN_CLASS, prog.getMainClassName());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testNoJarNoArgumentsAtAll() {
		try {
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			assertTrue(frontend.run(new String[0]) != 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testNonExistingFileWithArguments() {
		try {
			String[] arguments = {
					"--classpath", "file:///tmp/foo",
					"--classpath", "file:///tmp/bar",
					"/some/none/existing/path",
					"--debug", "true", "arg1", "arg2"  };
			URL[] classpath = new URL[] { new URL("file:///tmp/foo"), new URL("file:///tmp/bar") };
			String[] reducedArguments = {"--debug", "true", "arg1", "arg2"};

			RunOptions options = CliFrontendParser.parseRunCommand(arguments);
			assertEquals(arguments[4], options.getJarFilePath());
			assertArrayEquals(classpath, options.getClasspaths().toArray());
			assertArrayEquals(reducedArguments, options.getProgramArgs());

			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());

			try {
				frontend.buildProgram(options);
				fail("Should fail with an exception");
			}
			catch (FileNotFoundException e) {
				// that's what we want
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testNonExistingFileWithoutArguments() {
		try {
			String[] arguments = {"/some/none/existing/path"};

			RunOptions options = CliFrontendParser.parseRunCommand(arguments);
			assertEquals(arguments[0], options.getJarFilePath());
			assertArrayEquals(new String[0], options.getProgramArgs());

			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());

			try {
				frontend.buildProgram(options);
			}
			catch (FileNotFoundException e) {
				// that's what we want
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
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
	 *		at org.apache.flink.client.CliFrontend.info(CliFrontend.java:439)
	 *		at org.apache.flink.client.CliFrontend.parseParameters(CliFrontend.java:931)
	 *		at org.apache.flink.client.CliFrontend.main(CliFrontend.java:951)
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
	 * The test works as follows:
	 *
	 * <ul>
	 *   <li> Use the CliFrontend to invoke a jar file that loads a class which is only available
	 * 	      in the jarfile itself (via a custom classloader)
	 *   <li> Change the Usercode classloader of the PackagedProgram to a special classloader for this test
	 *   <li> the classloader will accept the special class (and return a String.class)
	 * </ul>
	 */
	@Test
	public void testPlanWithExternalClass() throws CompilerException, ProgramInvocationException {
		final boolean[] callme = { false }; // create a final object reference, to be able to change its val later

		try {
			String[] arguments = {
					"--classpath", "file:///tmp/foo",
					"--classpath", "file:///tmp/bar",
					"-c", TEST_JAR_CLASSLOADERTEST_CLASS, getTestJarPath(),
					"true", "arg1", "arg2" };
			URL[] classpath = new URL[] { new URL("file:///tmp/foo"), new URL("file:///tmp/bar") };
			String[] reducedArguments = { "true", "arg1", "arg2" };

			RunOptions options = CliFrontendParser.parseRunCommand(arguments);
			assertEquals(getTestJarPath(), options.getJarFilePath());
			assertArrayEquals(classpath, options.getClasspaths().toArray());
			assertEquals(TEST_JAR_CLASSLOADERTEST_CLASS, options.getEntryPointClassName());
			assertArrayEquals(reducedArguments, options.getProgramArgs());
			
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			PackagedProgram prog = spy(frontend.buildProgram(options));

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
			Client.getOptimizedPlanAsJson(compiler, prog, 666);
			fail("Should have failed with a ClassNotFoundException");
		}
		catch (ProgramInvocationException e) {
			if (!(e.getCause() instanceof ClassNotFoundException)) {
				e.printStackTrace();
				fail("Program didn't throw ClassNotFoundException");
			}
			assertTrue("Classloader was not called", callme[0]);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Program failed with the wrong exception: " + e.getClass().getName());
		}
	}
}
