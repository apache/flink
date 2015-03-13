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

package org.apache.flink.test.classloading;

import java.io.File;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

import org.junit.Assert;
import org.junit.Test;

public class InputSplitClassLoaderITCase {
	
	private static final String JAR_FILE = "target/customsplit-test-jar.jar";
	
	@Test
	public void testJobWithCustomInputFormat() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 2);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
			
			ForkableFlinkMiniCluster testCluster = new ForkableFlinkMiniCluster(config, false);
			try {
				int port = testCluster.getJobManagerRPCPort();

				PackagedProgram prog = new PackagedProgram(new File(JAR_FILE),
						new String[] { JAR_FILE, "localhost", String.valueOf(port) } );
				prog.invokeInteractiveModeForExecution();
			}
			finally {
				testCluster.shutdown();
			}
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}
}
