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

package eu.stratosphere.nephele.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class contains tests for the {@link DiscoveryService} class.
 * 
 * @author warneke
 */
public class DiscoveryServiceTest {

	/**
	 * The dummy IPC port used during the tests.
	 */
	private static final int IPC_PORT = 5555;

	/**
	 * Starts the discovery service before the tests.
	 * 
	 * @throws UnknownHostException
	 *         thrown if the {@link InetAddress} of localhost cannot be determined
	 * @throws DiscoveryException
	 *         thrown if an error occurs during the start of the discovery manager
	 */
	@BeforeClass
	public static void startService() throws UnknownHostException, DiscoveryException {

		final InetAddress bindAddress = InetAddress.getLocalHost();

		DiscoveryService.startDiscoveryService(bindAddress, IPC_PORT);
	}

	/**
	 * Tests the job manager discovery function.
	 */
	@Test
	public void testJobManagerDiscovery() {
		
		InetAddress localHost = null;
		
		try {
			localHost = InetAddress.getLocalHost();
		} catch(UnknownHostException e) {
			fail(e.getMessage());
		}
		
		try {
			final InetSocketAddress jobManagerAddress = DiscoveryService.getJobManagerAddress();
			
			assertEquals(localHost, jobManagerAddress.getAddress());
			assertEquals(IPC_PORT, jobManagerAddress.getPort());
			
		} catch(DiscoveryException e) {
			fail(e.getMessage());
		}
	}
	
	/**
	 * Tests if the task manager address resolution works properly.
	 */
	@Test
	public void testTaskManagerAddressResolution() {
		
		InetAddress localHost = null;
		
		try {
			localHost = InetAddress.getLocalHost();
		} catch(UnknownHostException e) {
			fail(e.getMessage());
		}
		
		try {
			final InetAddress taskManagerAddress = DiscoveryService.getTaskManagerAddress(localHost);
			
			assertEquals(localHost, taskManagerAddress);
			
		} catch(DiscoveryException e) {
			fail(e.getMessage());
		}
	}
	
	/**
	 * Shuts the discovery service down after the tests.
	 */
	@AfterClass
	public static void stopService() {

		DiscoveryService.stopDiscoveryService();
	}
}
