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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.Map;

import eu.stratosphere.configuration.GlobalConfiguration;

public class CliFrontendTestUtils {
	
	public static final String TEST_JAR_MAIN_CLASS = "eu.stratosphere.client.testjar.WordCount";
	
	public static final String TEST_JAR_CLASSLOADERTEST_CLASS = "eu.stratosphere.client.testjar.JobWithExternalDependency";
	
	
	public static final String TEST_JOB_MANAGER_ADDRESS = "192.168.1.33";
	
	public static final int TEST_JOB_MANAGER_PORT = 55443;
	
	public static final String TEST_YARN_JOB_MANAGER_ADDRESS = "22.33.44.55";
	
	public static final int TEST_YARN_JOB_MANAGER_PORT = 6655;
	
	
	public static String getTestJarPath() throws FileNotFoundException, MalformedURLException {
		File f = new File("target/maven-test-jar.jar");
		if(!f.exists()) {
			throw new FileNotFoundException("Test jar not present. Invoke tests using maven "
					+ "or build the jar using 'mvn process-test-classes' in stratosphere-clients");
		}
		return f.getAbsolutePath();
	}
	
	public static String getNonJarFilePath() {
		return CliFrontendRunTest.class.getResource("/testconfig/stratosphere-conf.yaml").getFile();
	}
	
	public static String getConfigDir() {
		String confFile = CliFrontendRunTest.class.getResource("/testconfig/stratosphere-conf.yaml").getFile();
		return new File(confFile).getAbsoluteFile().getParent();
	}
	
	public static String getInvalidConfigDir() {
		String confFile = CliFrontendRunTest.class.getResource("/invalidtestconfig/stratosphere-conf.yaml").getFile();
		return new File(confFile).getAbsoluteFile().getParent();
	}
	
	public static String getConfigDirWithYarnFile() {
		String confFile = CliFrontendRunTest.class.getResource("/testconfigwithyarn/stratosphere-conf.yaml").getFile();
		return new File(confFile).getAbsoluteFile().getParent();
	}
	
	public static String getConfigDirWithInvalidYarnFile() {
		String confFile = CliFrontendRunTest.class.getResource("/testconfigwithinvalidyarn/stratosphere-conf.yaml").getFile();
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
	
	public static class TestingCliFrontend extends CliFrontend {
		
		public final String configDir;
		
		public TestingCliFrontend() {
			this(getConfigDir());
		}
		
		public TestingCliFrontend(String configDir) {
			this.configDir = configDir;
		}
		
		@Override
		protected String getConfigurationDirectory() {
			return this.configDir;
		}
	}
	
	private static final class BlackholeOutputSteam extends java.io.OutputStream {
		@Override
		public void write(int b){}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private CliFrontendTestUtils() {}
}
