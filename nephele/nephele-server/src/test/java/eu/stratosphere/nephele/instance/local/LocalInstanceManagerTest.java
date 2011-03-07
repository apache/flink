/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.instance.local;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.discovery.DiscoveryException;
import eu.stratosphere.nephele.discovery.DiscoveryService;
import eu.stratosphere.nephele.instance.InstanceType;

/**
 * Tests for the {@link LocalInstanceManager}.
 * 
 * @author warneke
 */
public class LocalInstanceManagerTest {

	/**
	 * The system property key to retrieve the user directory.
	 */
	private static final String USER_DIR_KEY = "user.dir";

	/**
	 * The directory containing the correct configuration file to be used during the tests.
	 */
	private static final String CORRECT_CONF_DIR = "/correct-conf";

	/**
	 * Starts the discovery service before the tests.
	 */
	@BeforeClass
	public static void startDiscoveryService() {
		
		final String configDir = System.getProperty(USER_DIR_KEY) + CORRECT_CONF_DIR;
		
		GlobalConfiguration.loadConfiguration(configDir);
		
		final String address = GlobalConfiguration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		InetAddress bindAddress = null;
		if(address != null) {
			try {
				bindAddress = InetAddress.getByName(address);
			} catch(UnknownHostException e) {
				fail(e.getMessage());
			}
		}
		
		try {
			DiscoveryService.startDiscoveryService(bindAddress, 5555);
		} catch(DiscoveryException e) {
			fail(e.getMessage());
		}
	}
	
	/**
	 * Stops the discovery service after the tests.
	 */
	@AfterClass
	public static void stopDiscoveryService() {
		
		DiscoveryService.stopDiscoveryService();
	}
	
	/**
	 * Checks if the local instance manager reads the default correctly from the configuration file.
	 */
	@Test
	public void testInstanceTypeFromConfiguration() {

		final String configDir = System.getProperty(USER_DIR_KEY) + CORRECT_CONF_DIR;
		
		final TestInstanceListener testInstanceListener = new TestInstanceListener();

		LocalInstanceManager lm = null;
		try {

			lm = new LocalInstanceManager(configDir);
			lm.setInstanceListener(testInstanceListener);

			final InstanceType defaultInstanceType = lm.getDefaultInstanceType();
			assertEquals("test", defaultInstanceType.getIdentifier());
			assertEquals(4, defaultInstanceType.getNumberOfComputeUnits());
			assertEquals(4, defaultInstanceType.getNumberOfCores());
			assertEquals(1024, defaultInstanceType.getMemorySize());
			assertEquals(160, defaultInstanceType.getDiskCapacity());
			assertEquals(0, defaultInstanceType.getPricePerHour());

		} finally {

			if (lm != null) {
				lm.shutdown();
			}
		}
	}
}
