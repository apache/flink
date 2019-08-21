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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link HighAvailabilityServicesUtils} class.
 */
public class HighAvailabilityServicesUtilsTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testCreateCustomHAServices() throws Exception {
		Configuration config = new Configuration();

		HighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		TestHAFactory.haServices = haServices;

		Executor executor = Executors.directExecutor();

		config.setString(HighAvailabilityOptions.HA_MODE, TestHAFactory.class.getName());

		// when
		HighAvailabilityServices actualHaServices = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(config, executor);

		// then
		assertSame(haServices, actualHaServices);

		// when
		actualHaServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(config, executor,
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);
		// then
		assertSame(haServices, actualHaServices);
	}

	@Test
	public void testCreateCustomClientHAServices() throws Exception {
		Configuration config = new Configuration();

		ClientHighAvailabilityServices clientHAServices = TestingClientHAServices.createClientHAServices();
		TestHAFactory.clientHAServices = clientHAServices;

		config.setString(HighAvailabilityOptions.HA_MODE, TestHAFactory.class.getName());

		// when
		ClientHighAvailabilityServices actualClientHAServices = HighAvailabilityServicesUtils.createClientHAService(config);

		// then
		assertSame(clientHAServices, actualClientHAServices);
	}

	@Test(expected = Exception.class)
	public void testCustomHAServicesFactoryNotDefined() throws Exception {
		Configuration config = new Configuration();

		Executor executor = Executors.directExecutor();

		config.setString(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.FACTORY_CLASS.name().toLowerCase());

		// expect
		HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(config, executor);
	}

	@Test
	public void testGetClusterHighAvailableStoragePath() throws IOException {
		final String haStorageRootDirectory = temporaryFolder.newFolder().getAbsolutePath();
		final String clusterId = UUID.randomUUID().toString();
		final Configuration configuration = new Configuration();

		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haStorageRootDirectory);
		configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);

		final Path clusterHighAvailableStoragePath = HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration);

		final Path expectedPath = new Path(haStorageRootDirectory, clusterId);
		assertThat(clusterHighAvailableStoragePath, is(expectedPath));
	}

	/**
	 * Testing class which needs to be public in order to be instantiatable.
	 */
	public static class TestHAFactory implements HighAvailabilityServicesFactory {

		static HighAvailabilityServices haServices;
		static ClientHighAvailabilityServices clientHAServices;

		@Override
		public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor) {
			return haServices;
		}

		@Override
		public ClientHighAvailabilityServices createClientHAServices(Configuration configuration) throws Exception {
			return clientHAServices;
		}
	}
}
