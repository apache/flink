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

package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

import static org.apache.flink.mesos.configuration.MesosOptions.PORT_ASSIGNMENTS;
import static org.junit.Assert.assertEquals;

/**
 * Test that mesos config are extracted correctly from the configuration.
 */
public class LaunchableMesosWorkerTest extends TestLogger {

	@Test
	public void canGetPortKeys() {
		// Setup
		Configuration config = new Configuration();
		config.setString(PORT_ASSIGNMENTS, "someport.here,anotherport");

		// Act
		Set<String> portKeys = LaunchableMesosWorker.extractPortKeys(config);

		// Assert
		assertEquals("Must get right number of port keys", 4, portKeys.size());
		Iterator<String> iterator = portKeys.iterator();
		assertEquals("port key must be correct", LaunchableMesosWorker.TM_PORT_KEYS[0], iterator.next());
		assertEquals("port key must be correct", LaunchableMesosWorker.TM_PORT_KEYS[1], iterator.next());
		assertEquals("port key must be correct", "someport.here", iterator.next());
		assertEquals("port key must be correct", "anotherport", iterator.next());
	}

}
