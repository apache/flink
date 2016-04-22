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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.Map;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CliFrontendTestUtils {
	
	public static final String TEST_JAR_MAIN_CLASS = "org.apache.flink.client.testjar.WordCount";
	
	public static final String TEST_JAR_CLASSLOADERTEST_CLASS = "org.apache.flink.client.testjar.JobWithExternalDependency";

	public static final String TEST_JOB_MANAGER_ADDRESS = "192.168.1.33";

	public static final int TEST_JOB_MANAGER_PORT = 55443;
	
	
	public static String getTestJarPath() throws FileNotFoundException, MalformedURLException {
		File f = new File("target/maven-test-jar.jar");
		if(!f.exists()) {
			throw new FileNotFoundException("Test jar not present. Invoke tests using maven "
					+ "or build the jar using 'mvn process-test-classes' in flink-clients");
		}
		return f.getAbsolutePath();
	}
	
	public static String getNonJarFilePath() {
		return CliFrontendRunTest.class.getResource("/testconfig/flink-conf.yaml").getFile();
	}
	
	public static String getConfigDir() {
		String confFile = CliFrontendRunTest.class.getResource("/testconfig/flink-conf.yaml").getFile();
		return new File(confFile).getAbsoluteFile().getParent();
	}
	
	public static String getInvalidConfigDir() {
		String confFile = CliFrontendRunTest.class.getResource("/invalidtestconfig/flink-conf.yaml").getFile();
		return new File(confFile).getAbsoluteFile().getParent();
	}

	public static void pipeSystemOutToNull() {
		System.setOut(new PrintStream(new BlackholeOutputSteam()));
		System.setErr(new PrintStream(new BlackholeOutputSteam()));
	}
	
	public static void clearGlobalConfiguration() {
		try {
			Field singletonInstanceField = GlobalConfiguration.class.getDeclaredField("SINGLETON");
			Field conf = GlobalConfiguration.class.getDeclaredField("config");
			Field map = Configuration.class.getDeclaredField("confData");
			
			singletonInstanceField.setAccessible(true);
			conf.setAccessible(true);
			map.setAccessible(true);
			
			GlobalConfiguration gconf = (GlobalConfiguration) singletonInstanceField.get(null);
			if (gconf != null) {
				Configuration confObject = (Configuration) conf.get(gconf);
				@SuppressWarnings("unchecked")
				Map<String, Object> confData = (Map<String, Object>) map.get(confObject);
				confData.clear();
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test initialization caused an exception: " + e.getMessage());
		}
		
	}
	
	private static final class BlackholeOutputSteam extends java.io.OutputStream {
		@Override
		public void write(int b){}
	}

	public static void checkJobManagerAddress(Configuration config, String expectedAddress, int expectedPort) {
		String jobManagerAddress = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		int jobManagerPort = config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);

		assertEquals(expectedAddress, jobManagerAddress);
		assertEquals(expectedPort, jobManagerPort);
	}
	
	// --------------------------------------------------------------------------------------------
	
	private CliFrontendTestUtils() {}
}
