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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.apache.flink.client.CliFrontendTestUtils.getTestJarPath;
import static org.apache.flink.client.CliFrontendTestUtils.getConfigDir;
import static org.junit.Assert.fail;

public class CliFrontendLoggingTest {

	private static LocalFlinkMiniCluster cluster;
	private static Configuration config;
	private static String hostPort;
	private ByteArrayOutputStream stream = new ByteArrayOutputStream();
	private CliFrontend cli;
	private PrintStream output;

	@Before
	public void setUp() throws Exception {
		stream.reset();
		output = System.out;
		System.setOut(new PrintStream(stream));

		config = new Configuration();
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
		config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1);
		config.setBoolean(ConfigConstants.LOCAL_INSTANCE_MANAGER_START_WEBSERVER, false);
		hostPort = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null) + ":" +
				config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		try {
			cluster = new LocalFlinkMiniCluster(config, false, StreamingMode.BATCH_ONLY);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Setup of test actor system failed.");
		}

		cli = new CliFrontend(getConfigDir());
	}

	@After
	public void shutDownActorSystem() {
		System.setOut(output);
		if(cluster != null){
			cluster.shutdown();
		}
	}

	@Test
	public void verifyLogging(){
		try {
			int ret = cli.run(new String[]{"-m", hostPort, getTestJarPath()});
			System.out.flush();
			assert(ret == 0 && checkForLogs(stream.toString()));
		} catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			if(cluster != null){
				cluster.shutdown();
			}
		}
	}

	@Test
	public void verifyNoLogging(){
		try {
			int ret = cli.run(new String[]{"-q", "-m", hostPort, getTestJarPath()});
			System.out.flush();
			assert(ret == 0 && !checkForLogs(stream.toString()));
		} catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			if(cluster != null){
				cluster.shutdown();
			}
		}
	}

	private boolean checkForLogs(String output){
		return output.indexOf("RUNNING") >= 0;
	}
}
