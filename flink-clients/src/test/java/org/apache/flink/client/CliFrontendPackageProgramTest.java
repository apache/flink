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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class CliFrontendPackageProgramTest {
	
	@BeforeClass
	public static void init() {
		pipeSystemOutToNull();
	}

	@Test
	public void testNonExistingJarFile() {
		try {
			String[] arguments = {"-j", "/some/none/existing/path", "-a", "--verbose", "true", "arg1", "arg2"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, true);
				
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result == null);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testFileNotJarFile() {
		try {
			String[] arguments = {"-j", getNonJarFilePath(), "-a", "--verbose", "true", "arg1", "arg2"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, true);
			
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result == null);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testVariantWithExplicitJarAndArgumentsOption() {
		try {
			String[] arguments = {"-j", getTestJarPath(), "-a", "--verbose", "true", "arg1", "arg2"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, true);
			
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result instanceof PackagedProgram);
			
			PackagedProgram prog = (PackagedProgram) result;

			Assert.assertArrayEquals(new String[] {"--verbose", "true", "arg1", "arg2"}, prog.getArguments());
			Assert.assertEquals(TEST_JAR_MAIN_CLASS, prog.getMainClassName());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testVariantWithExplicitJarAndNoArgumentsOption() {
		try {
			String[] arguments = {"-j", getTestJarPath(), "--verbose", "true", "arg1", "arg2"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, true);
			
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result instanceof PackagedProgram);
			
			PackagedProgram prog = (PackagedProgram) result;

			Assert.assertArrayEquals(new String[] {"--verbose", "true", "arg1", "arg2"}, prog.getArguments());
			Assert.assertEquals(TEST_JAR_MAIN_CLASS, prog.getMainClassName());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testValidVariantWithNoJarAndNoArgumentsOption() {
		try {
			String[] arguments = {getTestJarPath(), "--verbose", "true", "arg1", "arg2"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, true);
			
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result instanceof PackagedProgram);
			
			PackagedProgram prog = (PackagedProgram) result;

			Assert.assertArrayEquals(new String[] {"--verbose", "true", "arg1", "arg2"}, prog.getArguments());
			Assert.assertEquals(TEST_JAR_MAIN_CLASS, prog.getMainClassName());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testNoJarNoArgumentsAtAll() {
		try {
			String[] arguments = {};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, true);
				
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result == null);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testNonExistingFileWithArguments() {
		try {
			String[] arguments = {"/some/none/existing/path", "--verbose", "true", "arg1", "arg2"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, true);
				
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result == null);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testNonExistingFileWithoutArguments() {
		try {
			String[] arguments = {"/some/none/existing/path"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, true);
				
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result == null);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
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
			String[] arguments = {"-j", getTestJarPath(), "-c", TEST_JAR_CLASSLOADERTEST_CLASS , "--verbose", "true", "arg1", "arg2"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, true);
			
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result instanceof PackagedProgram);

			PackagedProgram prog = spy((PackagedProgram) result);

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

			Assert.assertArrayEquals(new String[]{"--verbose", "true", "arg1", "arg2"}, prog.getArguments());
			Assert.assertEquals(TEST_JAR_CLASSLOADERTEST_CLASS, prog.getMainClassName());

			Configuration c = new Configuration();
			c.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "devil");
			Client cli = new Client(c, getClass().getClassLoader());
			
			cli.getOptimizedPlanAsJson(prog, 666);
		}
		catch(ProgramInvocationException pie) {
			assertTrue("Classloader was not called", callme[0]);
			// class not found exception is expected as some point
			if( ! ( pie.getCause() instanceof ClassNotFoundException ) ) {
				System.err.println(pie.getMessage());
				pie.printStackTrace();
				fail("Program caused an exception: " + pie.getMessage());
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			assertTrue("Classloader was not called", callme[0]);
			fail("Program caused an exception: " + e.getMessage());
		}
		
		
	}
}
