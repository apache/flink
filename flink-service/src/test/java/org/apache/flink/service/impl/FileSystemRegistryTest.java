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

package org.apache.flink.service.impl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.service.ServiceInstance;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Unit test for FileSystemRegistry.
 */
public class FileSystemRegistryTest {

	@Test
	public void testAddAndGet() throws IOException {

		FileSystemRegistry fileSystemRegistry = new FileSystemRegistry();
		File dir = File.createTempFile("file_registry", System.currentTimeMillis() + "");
		if (dir.exists()) {
			dir.delete();
		}
		dir.mkdirs();
		dir.deleteOnExit();
		Configuration configuration = new Configuration();
		configuration.setString(FileSystemRegistry.FILESYSTEM_REGISTRY_PATH, dir.getAbsolutePath());
		fileSystemRegistry.open(configuration);

		fileSystemRegistry.addInstance(
			"test_service",
			"1",
			"127.0.0.1",
			1011,
			new byte[1]
		);

		List<ServiceInstance> instances = fileSystemRegistry.getAllInstances("test_service");
		Assert.assertTrue(instances.size() == 1);
		Assert.assertEquals("test_service", instances.get(0).getServiceName());
		Assert.assertEquals("1", instances.get(0).getInstanceId());
		Assert.assertEquals("127.0.0.1", instances.get(0).getServiceIp());
		Assert.assertEquals(1011, instances.get(0).getServicePort());
		Assert.assertEquals("test_service", instances.get(0).getServiceName());
		Assert.assertArrayEquals(new byte[1], instances.get(0).getCustomData());
		fileSystemRegistry.close();
	}

	@Test
	public void testAddAndRemove() throws IOException {
		FileSystemRegistry fileSystemRegistry = new FileSystemRegistry();
		File dir = File.createTempFile("file_registry", System.currentTimeMillis() + "");
		if (dir.exists()) {
			dir.delete();
		}
		dir.mkdirs();
		dir.deleteOnExit();
		Configuration configuration = new Configuration();
		configuration.setString(FileSystemRegistry.FILESYSTEM_REGISTRY_PATH, dir.getAbsolutePath());
		fileSystemRegistry.open(configuration);

		fileSystemRegistry.addInstance(
			"test_service",
			"1",
			"127.0.0.1",
			1011,
			new byte[1]
		);

		fileSystemRegistry.removeInstance("test_service", "1");
		Assert.assertTrue(fileSystemRegistry.getAllInstances("test_service").isEmpty());
		fileSystemRegistry.close();
	}
}
