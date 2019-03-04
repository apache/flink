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
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.Executor;

import static org.junit.Assert.assertSame;

/**
 * Tests for the {@link HighAvailabilityServicesUtils} class.
 */
public class HighAvailabilityServicesUtilsTest extends TestLogger {

	@Test
	public void testCreateCustomHAServices() throws Exception {
		Configuration config = new Configuration();

		HighAvailabilityServices haServices = Mockito.mock(HighAvailabilityServices.class);
		TestHAFactory.haServices = haServices;

		Executor executor = Mockito.mock(Executor.class);

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

	@Test(expected = Exception.class)
	public void testCustomHAServicesFactoryNotDefined() throws Exception {
		Configuration config = new Configuration();

		Executor executor = Mockito.mock(Executor.class);

		config.setString(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.FACTORY_CLASS.name().toLowerCase());

		// expect
		HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(config, executor);
	}

	/**
	 * Testing class which needs to be public in order to be instantiatable.
	 */
	public static class TestHAFactory implements HighAvailabilityServicesFactory {

		static HighAvailabilityServices haServices;

		@Override
		public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor) {
			return haServices;
		}
	}
}
