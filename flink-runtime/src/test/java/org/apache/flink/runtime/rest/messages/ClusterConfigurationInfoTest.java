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

package org.apache.flink.runtime.rest.messages;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;

/**
 * Tests for the {@link ClusterConfigurationInfo}.
 */
public class ClusterConfigurationInfoTest extends RestResponseMarshallingTestBase<ClusterConfigurationInfo> {

	@Override
	protected Class<ClusterConfigurationInfo> getTestResponseClass() {
		return ClusterConfigurationInfo.class;
	}

	@Override
	protected ClusterConfigurationInfo getTestResponseInstance() {
		final ClusterConfigurationInfo expected = new ClusterConfigurationInfo(2);
		expected.add(new ClusterConfigurationInfoEntry<>("key-string", "value1"));
		expected.add(new ClusterConfigurationInfoEntry<>("key-int", 123));
		// any long value being below Integer.MAX_VALUE would cause a failure due to Jackson creating
		// an Integer out of it during deserialization
		expected.add(new ClusterConfigurationInfoEntry<>("key-long", 9876543210L));

		return expected;
	}

	@Test
	public void testClusterConfigurationInfoCreationWithString() {
		assertClusterConfigurationInfoEntryCreation("key", "value", "value");
	}

	@Test
	public void testMemoryRelatedClusterConfigurationInfoCreation() {
		for (String key : ClusterConfigurationInfo.MEMORY_OPTION_KEYS) {
			assertClusterConfigurationInfoEntryCreation(key, "1b", 1L);
		}
	}

	private void assertClusterConfigurationInfoEntryCreation(String key, String inputValue, Object expectedValue) {
		Assert.assertThat(ClusterConfigurationInfo.createClusterConfigurationInfoEntry(key, inputValue), is(new ClusterConfigurationInfoEntry<>(key, expectedValue)));
	}
}
