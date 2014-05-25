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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static eu.stratosphere.client.CliFrontendTestUtils.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.client.program.PackagedProgram;


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
}
