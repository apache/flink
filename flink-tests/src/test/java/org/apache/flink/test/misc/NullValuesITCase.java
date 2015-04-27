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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests how the system behaves when null records are passed through the system.
 */
@SuppressWarnings("serial")
public class NullValuesITCase {

	@Test
	public void testNullValues() {
		ForkableFlinkMiniCluster cluster = null;
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 7);
			cluster = new ForkableFlinkMiniCluster(config, false);

			ExecutionEnvironment env =
					ExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getJobManagerRPCPort());

			env.setParallelism(1);
			env.getConfig().disableSysoutLogging();

			DataSet<String> data = env.fromElements("hallo")
					.map(new MapFunction<String, String>() {
						@Override
						public String map(String value) throws Exception {
							return null;
						}
					});
			data.writeAsText("/tmp/myTest", FileSystem.WriteMode.OVERWRITE);

			try {
				env.execute();
				fail("this should fail due to null values.");
			}
			catch (ProgramInvocationException e) {
				assertNotNull(e.getCause());
				assertNotNull(e.getCause().getCause());
				assertTrue(e.getCause().getCause() instanceof NullPointerException);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (cluster != null) {
				cluster.shutdown();
			}
		}
	}
}
