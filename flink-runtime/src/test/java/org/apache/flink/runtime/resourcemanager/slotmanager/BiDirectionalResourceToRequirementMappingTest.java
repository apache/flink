/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Test for the {@link BiDirectionalResourceToRequirementMapping}.
 */
public class BiDirectionalResourceToRequirementMappingTest extends TestLogger {

	@Test
	public void testIncrement() {
		BiDirectionalResourceToRequirementMapping mapping = new BiDirectionalResourceToRequirementMapping();

		ResourceProfile requirement = ResourceProfile.UNKNOWN;
		ResourceProfile resource = ResourceProfile.ANY;

		mapping.incrementCount(requirement, resource, 1);

		assertThat(mapping.getRequirementsFulfilledBy(resource).get(requirement), is(1));
		assertThat(mapping.getResourcesFulfilling(requirement).get(resource), is(1));

		assertThat(mapping.getAllRequirementProfiles(), contains(requirement));
		assertThat(mapping.getAllResourceProfiles(), contains(resource));
	}

	@Test
	public void testDecrement() {
		BiDirectionalResourceToRequirementMapping mapping = new BiDirectionalResourceToRequirementMapping();

		ResourceProfile requirement = ResourceProfile.UNKNOWN;
		ResourceProfile resource = ResourceProfile.ANY;

		mapping.incrementCount(requirement, resource, 1);
		mapping.decrementCount(requirement, resource, 1);

		assertThat(mapping.getRequirementsFulfilledBy(resource).getOrDefault(requirement, 0), is(0));
		assertThat(mapping.getResourcesFulfilling(requirement).getOrDefault(resource, 0), is(0));

		assertThat(mapping.getAllRequirementProfiles(), empty());
		assertThat(mapping.getAllResourceProfiles(), empty());

		assertThat(mapping.isEmpty(), is(true));
	}

}
