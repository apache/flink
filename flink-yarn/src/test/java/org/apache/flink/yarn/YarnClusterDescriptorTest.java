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
package org.apache.flink.yarn;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class YarnClusterDescriptorTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private File flinkJar;
	private File flinkConf;

	@Before
	public void beforeTest() throws IOException {
		temporaryFolder.create();
		flinkJar = temporaryFolder.newFile("flink.jar");
		flinkConf = temporaryFolder.newFile("flink-conf.yaml");
	}

	@Test
	public void testFailIfTaskSlotsHigherThanMaxVcores() {

		YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor();

		clusterDescriptor.setLocalJarPath(new Path(flinkJar.getPath()));
		clusterDescriptor.setFlinkConfiguration(new Configuration());
		clusterDescriptor.setConfigurationDirectory(temporaryFolder.getRoot().getAbsolutePath());
		clusterDescriptor.setConfigurationFilePath(new Path(flinkConf.getPath()));

		// configure slots too high
		clusterDescriptor.setTaskManagerSlots(Integer.MAX_VALUE);

		try {
			clusterDescriptor.deploy();
		} catch (Exception e) {
			Assert.assertTrue(e.getCause() instanceof IllegalConfigurationException);
		}
	}

	@Test
	public void testConfigOverwrite() {

		YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor();

		Configuration configuration = new Configuration();
		// overwrite vcores in config
		configuration.setInteger(ConfigConstants.YARN_VCORES, Integer.MAX_VALUE);

		clusterDescriptor.setLocalJarPath(new Path(flinkJar.getPath()));
		clusterDescriptor.setFlinkConfiguration(configuration);
		clusterDescriptor.setConfigurationDirectory(temporaryFolder.getRoot().getAbsolutePath());
		clusterDescriptor.setConfigurationFilePath(new Path(flinkConf.getPath()));

		// configure slots
		clusterDescriptor.setTaskManagerSlots(1);

		try {
			clusterDescriptor.deploy();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(e.getCause() instanceof IllegalConfigurationException);
		}
	}
}
