/**
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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnClientIT extends YarnTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(YarnClientIT.class);

	/*
	Should run cleanly, parameters are correct
	 */
	@Test
	public void testClientStartup() {
		ClientCallableExTesting client = new ClientCallableExTesting();
		client.addParameter("-confDir", flinkConfFile.getParentFile().getAbsolutePath());
		client.addParameter("-j", uberJarLocation);
		client.addParameter("-n", "1");
		client.addParameter("-jm", "128");
		client.addParameter("-tm", "1024");

		Exception ex = executeExClient(client, 15000);

		if (ex != null) {
			ex.printStackTrace();
			Assert.fail();
		}
	}

	/*
	Should run cleanly, parameters are correct
	 */
	@Test
	public void testClientStartupTwoContainer() {
		ClientCallableExTesting client = new ClientCallableExTesting();
		client.addParameter("-confDir", flinkConfFile.getParentFile().getAbsolutePath());
		client.addParameter("-j", uberJarLocation);
		client.addParameter("-n", "2");
		client.addParameter("-jm", "128");
		client.addParameter("-tm", "1024");

		Exception ex = executeExClient(client, 15000);

		if (ex != null) {
			ex.printStackTrace();
			Assert.fail();
		}
	}

	/*
	Should throw IllegalArgumentException, JobManager Memory configured too low
	 */
	@Test
	public void testJobManagerLowMemory() {
		ClientCallableExTesting client = new ClientCallableExTesting();
		client.addParameter("-confDir", flinkConfFile.getParentFile().getAbsolutePath());
		client.addParameter("-j", uberJarLocation);
		client.addParameter("-n", "1");
		client.addParameter("-jm", "100"); //not enough
		client.addParameter("-tm", "1024");

		Exception ex = executeExClient(client, 15000);

		if (ex == null) {
			Assert.fail("Test not successful, should have thrown RunTimeException");
		}
	}

	/*
	Should throw IllegalArgumentException, TaskManager Memory configured too low
	 */
	@Test
	public void testTaskManagerLowMemory() {
		ClientCallableExTesting client = new ClientCallableExTesting();
		client.addParameter("-j", uberJarLocation);
		client.addParameter("-n", "1");
		client.addParameter("-jm", "512"); //not enough
		client.addParameter("-tm", "50");

		Exception ex = executeExClient(client, 15000);

		if (ex == null) {
			Assert.fail("Test not successful, should have thrown RunTimeException");
		}
	}

	/*
	Should throw IllegalArgumentException, TaskManager Memory configured too low
	 */
	@Test
	public void testTaskManagerHighMemory() {
		ClientCallableExTesting client = new ClientCallableExTesting();
		client.addParameter("-j", uberJarLocation);
		client.addParameter("-n", "1");
		client.addParameter("-jm", new Integer(500*1024).toString()); //too much
		client.addParameter("-tm", "1024");

		Exception ex = executeExClient(client, 15000);

		if (ex == null) {
			Assert.fail("Test not successful, should have thrown RunTimeException");
		}
	}

	/*
	Should throw IllegalArgumentException, TaskManager Memory configured too low
	 */
	@Test
	public void testJobManagerHighMemory() {
		ClientCallableExTesting client = new ClientCallableExTesting();
		client.addParameter("-j", uberJarLocation);
		client.addParameter("-n", "1");
		client.addParameter("-jm", "512"); //too much
		client.addParameter("-tm", new Integer(500*1024).toString());

		Exception ex = executeExClient(client, 15000);

		if (ex == null) {
			Assert.fail("Test not successful, should have thrown RunTimeException");
		}
	}
	/*
	Should throw IllegalArgumentException, TaskManager Memory configured too low
	 */
	@Test
	public void testTaskManagerHighCores() {
		ClientCallableExTesting client = new ClientCallableExTesting();
		client.addParameter("-j", uberJarLocation);
		client.addParameter("-n", "1");
		client.addParameter("-jm", "512");
		client.addParameter("-tm", "1024");
		client.addParameter("-tmc", "1024"); //too many cores

		Exception ex = executeExClient(client, 15000);

		if (ex == null) {
			Assert.fail("Test not successful, should have thrown RunTimeException");
		}
	}

	/*
	Should throw MissingArgumentException, TaskManager Memory configured too low
	 */
	@Test
	public void testNoContainerArgument() {
		ClientCallableExTesting client = new ClientCallableExTesting();
		client.addParameter("-j", uberJarLocation);
		client.addParameter("-jm", "512"); //not enough
		client.addParameter("-tm", "50");

		Exception ex = executeExClient(client, 15000);

		if (ex == null) {
			Assert.fail("Test not successful, should have thrown RunTimeException");
		}
	}
}
