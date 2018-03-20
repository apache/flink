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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests to guard {@link ResourceManagerRuntimeServices}.
 */
public class ResourceManagerRuntimeServicesTest {

	/**
	 * Test to guard {@link ResourceManagerRuntimeServices#calculateCutoffMB(Configuration, long)}.
	 */
	@Test
	public void calculateCutoffMB() throws Exception {

		Configuration config = new Configuration();
		long containerMemoryMB = 1000;

		config.setFloat(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO, 0.1f);
		config.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, 128);

		Assert.assertEquals(128, ResourceManagerRuntimeServices.calculateCutoffMB(config, containerMemoryMB));

		config.setFloat(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO, 0.2f);
		Assert.assertEquals(200, ResourceManagerRuntimeServices.calculateCutoffMB(config, containerMemoryMB));

		config.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, 1000);

		try {
			ResourceManagerRuntimeServices.calculateCutoffMB(config, containerMemoryMB);
		} catch (Exception expected) {
			// we expected it.
			return;
		}
		Assert.fail();
	}
}
