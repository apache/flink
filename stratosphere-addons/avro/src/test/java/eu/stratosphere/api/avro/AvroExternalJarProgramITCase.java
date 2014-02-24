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
package eu.stratosphere.api.avro;

import java.io.File;
import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.PackagedProgram;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.LogUtils;


public class AvroExternalJarProgramITCase {

	private static final int TEST_JM_PORT = 43191;
	
	private static final String JAR_FILE = "/AvroTestProgram.jar";
	
	private static final String TEST_DATA_FILE = "/testdata.avro";

	static {
		LogUtils.initializeDefaultConsoleLogger();
	}
	
	@Test
	public void testExternalProgram() {
		
		NepheleMiniCluster testMiniCluster = null;
		
		try {
			testMiniCluster = new NepheleMiniCluster();
			testMiniCluster.setJobManagerRpcPort(TEST_JM_PORT);
			testMiniCluster.start();
			
			String jarFile = getClass().getResource(JAR_FILE).getFile();
			String testData = getClass().getResource(TEST_DATA_FILE).toString();
			
			PackagedProgram program = new PackagedProgram(new File(jarFile), new String[] { testData });
						
			Client c = new Client(new InetSocketAddress("localhost", TEST_JM_PORT), new Configuration());
			c.run(program.getPlanWithJars(), true);
		}
		catch (Throwable t) {
			System.err.println(t.getMessage());
			t.printStackTrace();
			Assert.fail("Error during the packaged program execution: " + t.getMessage());
		}
		finally {
			if (testMiniCluster != null) {
				try {
					testMiniCluster.stop();
				} catch (Throwable t) {}
			}
		}
	}
}
