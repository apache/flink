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

import static org.junit.Assert.*;

import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.PackagedProgram;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;


public class CliFrontendTest {

	private static final String TEST_JAR_MAIN_CLASS = "eu.stratosphere.example.java.wordcount.WordCount";
	
	private static final String TEST_JOB_MANAGER_ADDRESS = "192.168.1.33";
	
	private static final int TEST_JOB_MANAGER_PORT = 55443;
	
	private static final String TEST_WEBFRONTEND_ADDRESS = "192.168.1.33:" + ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT;
	
	
	@BeforeClass
	public static void init() {
		pipeSystemOutToNull();
		clearGlobalConfiguration();
	}
	
	
	@Test
	public void testRun() {
		try {
			// test unrecognized option
			{
				String[] parameters = {"-w", "-v", "-l", "-a", "some", "program", "arguments"};
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				int retCode = testFrontend.run(parameters);
				assertTrue(retCode == 2);
			}
			
			// test non existing jar file
			{
				String[] arguments = {"-j", "/some/none/existing/path", "-a", "plenty", "of", "arguments"};
				TestingCliFrontend frontend = new TestingCliFrontend();
				int returnCode = frontend.run(arguments);
				assertTrue(returnCode != 0);
			}
			
			// test file not jar file 
			{
				String[] arguments = {"-j", getNonJarFilePath(), "-a", "plenty", "of", "arguments"};
				TestingCliFrontend frontend = new TestingCliFrontend();
				int returnCode = frontend.run(arguments);
				assertTrue(returnCode != 0);
			}
			
			// test variant with explicit jar and arguments option
			{
				String[] parameters = {"-w", "-v", "-j", getTestJarPath(), "-a", "some", "program", "arguments"};
				
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				testFrontend.expectedArguments = new String[] {"some", "program", "arguments"};
				testFrontend.expectedMainClass = TEST_JAR_MAIN_CLASS;
				testFrontend.expectedWait = true;
				testFrontend.expectedWebFrontend = TEST_WEBFRONTEND_ADDRESS;
				
				assertEquals(0, testFrontend.run(parameters));
			}
			
			// test valid variant with explicit jar and no arguments option
			{
				String[] parameters = {"-w", "-v", "-j", getTestJarPath(), "some", "program", "arguments"};
				
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				testFrontend.expectedArguments = new String[] {"some", "program", "arguments"};
				testFrontend.expectedMainClass = TEST_JAR_MAIN_CLASS;
				testFrontend.expectedWait = true;
				
				assertEquals(0, testFrontend.run(parameters));
			}
			
			// test valid variant with no jar and no arguments option
			{
				String[] parameters = {"-w", "-v", getTestJarPath(), "some", "program", "arguments"};
				
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				testFrontend.expectedArguments = new String[] {"some", "program", "arguments"};
				testFrontend.expectedMainClass = TEST_JAR_MAIN_CLASS;
				testFrontend.expectedWait = true;
				testFrontend.expectedWebFrontend = TEST_WEBFRONTEND_ADDRESS;
				
				assertEquals(0, testFrontend.run(parameters));
			}
			
			// test no jar no arguments at all
			{
				String[] parameters = {"-v"};
				
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				assertTrue(0 != testFrontend.run(parameters));
			}
			
			// test non existing file without arguments
			{
				String[] parameters = {"-w", "-v", "/some/none/existing/path", "some", "program", "arguments"};
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				assertTrue(0 != testFrontend.run(parameters));
			}

			// test non existing file without arguments
			{
				String[] parameters = {"-w", "-v", "/some/none/existing/path", "some", "program", "arguments"};
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				assertTrue(0 != testFrontend.run(parameters));
			}
			
			// test configure parallelism
			{
				String[] parameters = {"-v", "-p", "42",  getTestJarPath()};
				
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				testFrontend.expectedArguments = new String[0];
				testFrontend.expectedMainClass = TEST_JAR_MAIN_CLASS;
				testFrontend.expectedWait = false;
				testFrontend.expectedDop = 42;
				
				assertEquals(0, testFrontend.run(parameters));
			}
			
			// test configure parallelism with non integer value
			{
				String[] parameters = {"-v", "-p", "text",  getTestJarPath()};
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				assertTrue(0 != testFrontend.run(parameters));
			}
			
			// test configure parallelism with overflow integer value
			{
				String[] parameters = {"-v", "-p", "475871387138",  getTestJarPath()};
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				assertTrue(0 != testFrontend.run(parameters));
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testRunWithJobManagerOption() {
		try {
			// test with job manager option
			{
				String[] parameters = {"-m", "12.13.14.15:5454", "-j", getTestJarPath(), "-a", "some", "program", "arguments"};
				
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				testFrontend.expectedArguments = new String[] {"some", "program", "arguments"};
				testFrontend.expectedMainClass = TEST_JAR_MAIN_CLASS;
				testFrontend.expectedJobManagerAddress = "12.13.14.15";
				testFrontend.expectedJobManagerPort = 5454;
				
				assertEquals(0, testFrontend.run(parameters));
			}
			
			// test with non-parsable job manager option
			{
				String[] parameters = {"-m", "garbage", "-j", getTestJarPath(), "-a", "some", "program", "arguments"};
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				assertTrue(0 != testFrontend.run(parameters));
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static String getTestJarPath() {
		return CliFrontendTest.class.getResource("/test.jar").getFile();
	}
	
	public static String getNonJarFilePath() {
		return CliFrontendTest.class.getResource("/testconfig/stratosphere-conf.yaml").getFile();
	}
	
	public static String getConfigDir() {
		String confFile = CliFrontendTest.class.getResource("/testconfig/stratosphere-conf.yaml").getFile();
		return new File(confFile).getAbsoluteFile().getParent();
	}
	
	public static String getInvalidConfigDir() {
		String confFile = CliFrontendTest.class.getResource("/invalidtestconfig/stratosphere-conf.yaml").getFile();
		return new File(confFile).getAbsoluteFile().getParent();
	}
	
	public static String getConfigDirWithYarnFile() {
		String confFile = CliFrontendTest.class.getResource("/testconfigwithyarn/stratosphere-conf.yaml").getFile();
		return new File(confFile).getAbsoluteFile().getParent();
	}
	
	public static String getConfigDirWithInvalidYarnFile() {
		String confFile = CliFrontendTest.class.getResource("/testconfigwithinvalidyarn/stratosphere-conf.yaml").getFile();
		return new File(confFile).getAbsoluteFile().getParent();
	}
	
	public static void pipeSystemOutToNull() {
		System.setOut(new PrintStream(new BlackholeOutputSteam()));
	}
	
	public static void clearGlobalConfiguration() {
		try {
			Field singletonInstanceField = GlobalConfiguration.class.getDeclaredField("configuration");
			Field confDataMapField = GlobalConfiguration.class.getDeclaredField("confData");
			
			singletonInstanceField.setAccessible(true);
			confDataMapField.setAccessible(true);
			
			GlobalConfiguration gconf = (GlobalConfiguration) singletonInstanceField.get(null);
			if (gconf != null) {
				@SuppressWarnings("unchecked")
				Map<String, String> confData = (Map<String, String>) confDataMapField.get(gconf);
				confData.clear();
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test initialization caused an exception: " + e.getMessage());
		}
		
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class TestingCliFrontend extends CliFrontend {
		
		public final String configDir;
		
		public String expectedJobManagerAddress;
		
		public int expectedJobManagerPort;
		
		public String[] expectedArguments;
		
		public String expectedMainClass;
		
		public int expectedDop;
		
		public String expectedWebFrontend;
		
		public boolean expectedWait;
		
		
		public TestingCliFrontend() {
			this(getConfigDir());
		}
		
		public TestingCliFrontend(String configDir) {
			this.configDir = configDir;
		}
		
		@Override
		protected int executeProgram(PackagedProgram program, Client client, int parallelism, boolean wait, String webFrontendAddress) {
			if (expectedJobManagerAddress != null) {
				assertEquals(expectedJobManagerAddress, client.getJobManagerAddress());
			} else {
				assertEquals(TEST_JOB_MANAGER_ADDRESS, client.getJobManagerAddress());
			}
			
			if (expectedJobManagerPort > 0) {
				assertEquals(expectedJobManagerPort, client.getJobManagerPort());
			} else {
				assertEquals(TEST_JOB_MANAGER_PORT, client.getJobManagerPort());
			}
			
			if (expectedArguments != null) {
				assertArrayEquals(expectedArguments, program.getArguments());
			}
			if (expectedMainClass != null) {
				assertEquals(expectedMainClass, program.getMainClassName());
			}
			if (expectedDop > 0) {
				assertEquals(expectedDop, parallelism);
			}
			if (expectedWebFrontend != null) {
				assertEquals(expectedWebFrontend, webFrontendAddress);
			}
			assertEquals(expectedWait, wait);
			
			return 0;
		}
		
		
		@Override
		protected String getConfigurationDirectory() {
			return this.configDir;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class BlackholeOutputSteam extends java.io.OutputStream {
		@Override
		public void write(int b){}
	}
}
