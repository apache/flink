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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HighAvailabilityModeTest {

	// Default HA mode
	private final static HighAvailabilityMode DEFAULT_HA_MODE = HighAvailabilityMode.valueOf(
			ConfigConstants.DEFAULT_HA_MODE.toUpperCase());

	/**
	 * Tests HA mode configuration.
	 */
	@Test
	public void testFromConfig() throws Exception {
		Configuration config = new Configuration();

		// Check default
		assertEquals(DEFAULT_HA_MODE, HighAvailabilityMode.fromConfig(config));

		// Check not equals default
		config.setString(ConfigConstants.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name().toLowerCase());
		assertEquals(HighAvailabilityMode.ZOOKEEPER, HighAvailabilityMode.fromConfig(config));
	}

	/**
	 * Tests HA mode configuration with deprecated config values.
	 */
	@Test
	public void testDeprecatedFromConfig() throws Exception {
		Configuration config = new Configuration();

		// Check mapping of old default to new default
		config.setString(ConfigConstants.RECOVERY_MODE, ConfigConstants.DEFAULT_RECOVERY_MODE);
		assertEquals(DEFAULT_HA_MODE, HighAvailabilityMode.fromConfig(config));

		// Check deprecated config
		config.setString(ConfigConstants.RECOVERY_MODE, HighAvailabilityMode.ZOOKEEPER.name().toLowerCase());
		assertEquals(HighAvailabilityMode.ZOOKEEPER, HighAvailabilityMode.fromConfig(config));

		// Check precedence over deprecated config
		config.setString(ConfigConstants.HA_MODE, HighAvailabilityMode.NONE.name().toLowerCase());
		config.setString(ConfigConstants.RECOVERY_MODE, HighAvailabilityMode.ZOOKEEPER.name().toLowerCase());

		assertEquals(HighAvailabilityMode.NONE, HighAvailabilityMode.fromConfig(config));
	}

}
