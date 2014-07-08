/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.client;

import static eu.stratosphere.client.CliFrontendTestUtils.TEST_JAR_CLASSLOADERTEST_CLASS;
import static eu.stratosphere.client.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static eu.stratosphere.client.CliFrontendTestUtils.getNonJarFilePath;
import static eu.stratosphere.client.CliFrontendTestUtils.getTestJarPath;
import static eu.stratosphere.client.CliFrontendTestUtils.pipeSystemOutToNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.PackagedProgram;
import eu.stratosphere.client.program.ProgramInvocationException;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;


public class CliFrontendPackageProgramTest {
	
	@BeforeClass
	public static void init() {
		pipeSystemOutToNull();
	}

	@Test
	public void testNonExistingJarFile() {
		try {
			String[] arguments = {"-j", "/some/none/existing/path", "-a", "plenty", "of", "arguments"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, false);
				
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
			String[] arguments = {"-j", getNonJarFilePath(), "-a", "plenty", "of", "arguments"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), arguments, false);
			
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
			String[] parameters = {"-j", getTestJarPath(), "-a", "some", "program", "arguments"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), parameters, false);
			
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result instanceof PackagedProgram);
			
			PackagedProgram prog = (PackagedProgram) result;
			
			Assert.assertArrayEquals(new String[] {"some", "program", "arguments"}, prog.getArguments());
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
			String[] parameters = {"-j", getTestJarPath(), "some", "program", "arguments"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), parameters, false);
			
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result instanceof PackagedProgram);
			
			PackagedProgram prog = (PackagedProgram) result;
			
			Assert.assertArrayEquals(new String[] {"some", "program", "arguments"}, prog.getArguments());
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
			String[] parameters = {getTestJarPath(), "some", "program", "arguments"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), parameters, false);
			
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result instanceof PackagedProgram);
			
			PackagedProgram prog = (PackagedProgram) result;
			
			Assert.assertArrayEquals(new String[] {"some", "program", "arguments"}, prog.getArguments());
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
			String[] parameters = {};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), parameters, false);
				
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
			String[] parameters = {"/some/none/existing/path", "some", "program", "arguments"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), parameters, false);
				
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
			String[] parameters = {"/some/none/existing/path"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), parameters, false);
				
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
	 * The test works as follows:
	 * - Use the CliFrontend to invoke a jar file that loads a class which is only available
	 * 	in the jarfile itself (via a custom classloader)
	 * - Change the Usercode classloader of the PackagedProgram to a special classloader for this test
	 * - the classloader will accept the special class (and return a String.class)
	 * 
	 * 	eu.stratosphere.client.program.ProgramInvocationException: The main method caused an error.
		at eu.stratosphere.client.program.PackagedProgram.callMainMethod(PackagedProgram.java:398)
		at eu.stratosphere.client.program.PackagedProgram.invokeInteractiveModeForExecution(PackagedProgram.java:301)
		at eu.stratosphere.client.program.Client.getOptimizedPlan(Client.java:140)
		at eu.stratosphere.client.program.Client.getOptimizedPlanAsJson(Client.java:125)
		at eu.stratosphere.client.CliFrontend.info(CliFrontend.java:439)
		at eu.stratosphere.client.CliFrontend.parseParameters(CliFrontend.java:931)
		at eu.stratosphere.client.CliFrontend.main(CliFrontend.java:951)
	Caused by: java.io.IOException: java.lang.RuntimeException: java.lang.ClassNotFoundException: org.apache.hadoop.hive.ql.io.RCFileInputFormat
		at org.apache.hcatalog.mapreduce.HCatInputFormat.setInput(HCatInputFormat.java:102)
		at org.apache.hcatalog.mapreduce.HCatInputFormat.setInput(HCatInputFormat.java:54)
		at tlabs.CDR_In_Report.createHCatInputFormat(CDR_In_Report.java:322)
		at tlabs.CDR_Out_Report.main(CDR_Out_Report.java:380)
		at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
		at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
		at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
		at java.lang.reflect.Method.invoke(Method.java:622)
		at eu.stratosphere.client.program.PackagedProgram.callMainMethod(PackagedProgram.java:383)

	 */
	@Test
	public void testPlanWithExternalClass() throws CompilerException, ProgramInvocationException {
		final Boolean callme[] = { false }; // create a final object reference, to be able to change its val later
		try {
			String[] parameters = {getTestJarPath(), "-c", TEST_JAR_CLASSLOADERTEST_CLASS , "some", "program"};
			CommandLine line = new PosixParser().parse(CliFrontend.getProgramSpecificOptions(new Options()), parameters, false);
			
			CliFrontend frontend = new CliFrontend();
			Object result = frontend.buildProgram(line);
			assertTrue(result instanceof PackagedProgram);
			
			PackagedProgram prog = spy((PackagedProgram) result);

			ClassLoader testClassLoader = new ClassLoader(prog.getUserCodeClassLoader()) {
				@Override
				public Class<?> loadClass(String name) throws ClassNotFoundException {
					assertTrue(name.equals("org.apache.hadoop.hive.ql.io.RCFileInputFormat"));
					callme[0] = true;
					return String.class; // Intentionally return the wrong class.
				}
			};
			when(prog.getUserCodeClassLoader()).thenReturn(testClassLoader);

			Assert.assertArrayEquals(new String[]{"some", "program"}, prog.getArguments());
			Assert.assertEquals(TEST_JAR_CLASSLOADERTEST_CLASS, prog.getMainClassName());

			Configuration c = new Configuration();
			c.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "devil");
			Client cli = new Client(c);
			
			cli.getOptimizedPlanAsJson(prog, 666);
		} catch(ProgramInvocationException pie) {
			assertTrue("Classloader was not called", callme[0]);
			// class not found exception is expected as some point
			if( ! ( pie.getCause() instanceof ClassNotFoundException ) ) {
				System.err.println(pie.getMessage());
				pie.printStackTrace();
				fail("Program caused an exception: " + pie.getMessage());
			}
		}
		catch (Exception e) {
			assertTrue("Classloader was not called", callme[0]);
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
		
		
	}
}
