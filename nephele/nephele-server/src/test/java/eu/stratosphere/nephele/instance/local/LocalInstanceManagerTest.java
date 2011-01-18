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

import org.junit.Test;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.util.TestInstanceListener;

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
	 * Checks if the local instance manager reads the default correctly from the configuration file.
	 */
	@Test
	public void testInstanceTypeFromConfiguration() {

		final String configDir = System.getProperty(USER_DIR_KEY) + CORRECT_CONF_DIR;

		GlobalConfiguration.loadConfiguration(configDir);
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
