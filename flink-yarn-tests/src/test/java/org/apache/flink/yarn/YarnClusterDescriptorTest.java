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
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class YarnClusterDescriptorTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests to ship a lib folder through the {@code YarnClusterDescriptor.addShipFiles}
	 */
	@Test
	public void testExplicitLibShipping() throws Exception {
		AbstractYarnClusterDescriptor descriptor = new YarnClusterDescriptor();
		descriptor.setLocalJarPath(new Path("/path/to/flink.jar"));

		descriptor.setConfigurationDirectory(temporaryFolder.getRoot().getAbsolutePath());
		descriptor.setConfigurationFilePath(new Path(temporaryFolder.getRoot().getPath()));
		descriptor.setFlinkConfiguration(new Configuration());

		File libFile = temporaryFolder.newFile("libFile.jar");
		File libFolder = temporaryFolder.newFolder().getAbsoluteFile();

		Assert.assertFalse(descriptor.shipFiles.contains(libFile));
		Assert.assertFalse(descriptor.shipFiles.contains(libFolder));

		List<File> shipFiles = new ArrayList<>();
		shipFiles.add(libFile);
		shipFiles.add(libFolder);

		descriptor.addShipFiles(shipFiles);

		Assert.assertTrue(descriptor.shipFiles.contains(libFile));
		Assert.assertTrue(descriptor.shipFiles.contains(libFolder));

		// only execute part of the deployment to test for shipped files
		Set<File> effectiveShipFiles = new HashSet<>();
		descriptor.addLibFolderToShipFiles(effectiveShipFiles);

		Assert.assertEquals(0, effectiveShipFiles.size());
		Assert.assertEquals(2, descriptor.shipFiles.size());
		Assert.assertTrue(descriptor.shipFiles.contains(libFile));
		Assert.assertTrue(descriptor.shipFiles.contains(libFolder));
	}

	/**
	 * Tests to ship a lib folder through the {@code ConfigConstants.ENV_FLINK_LIB_DIR}
	 */
	@Test
	public void testEnvironmentLibShipping() throws Exception {
		AbstractYarnClusterDescriptor descriptor = new YarnClusterDescriptor();

		descriptor.setConfigurationDirectory(temporaryFolder.getRoot().getAbsolutePath());
		descriptor.setConfigurationFilePath(new Path(temporaryFolder.getRoot().getPath()));
		descriptor.setFlinkConfiguration(new Configuration());

		File libFolder = temporaryFolder.newFolder().getAbsoluteFile();
		File libFile = new File(libFolder, "libFile.jar");
		libFile.createNewFile();

		Set<File> effectiveShipFiles = new HashSet<>();

		final Map<String, String> oldEnv = System.getenv();
		try {
			Map<String, String> env = new HashMap<>(1);
			env.put(ConfigConstants.ENV_FLINK_LIB_DIR, libFolder.getAbsolutePath());
			TestBaseUtils.setEnv(env);
			// only execute part of the deployment to test for shipped files
			descriptor.addLibFolderToShipFiles(effectiveShipFiles);
		} finally {
			TestBaseUtils.setEnv(oldEnv);
		}

		// only add the ship the folder, not the contents
		Assert.assertFalse(effectiveShipFiles.contains(libFile));
		Assert.assertTrue(effectiveShipFiles.contains(libFolder));
		Assert.assertFalse(descriptor.shipFiles.contains(libFile));
		Assert.assertFalse(descriptor.shipFiles.contains(libFolder));
	}

}
