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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.AbstractID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link JobPendingSlotRequestDetail}.
 */
public class JobPendingSlotRequestDetailTest {

	/**
	 * Tests that we can marshal and unmarshal JobDetails instances.
	 */
	@Test
	public void testPendingSlotRequestDetailMarshalling() throws JsonProcessingException {
		// case
		{
			Map<String, Resource> extendedResources = new HashMap<>();
			extendedResources.put(ResourceSpec.MANAGED_MEMORY_NAME, new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, 56));
			final JobPendingSlotRequestDetail expected = new JobPendingSlotRequestDetail(
					new SlotRequestId(),
					new ResourceProfile(0.123, 12, 23, 34, 45, extendedResources),
					625L,
					new SlotSharingGroupId(),
					new AbstractID(),
					Lists.newArrayList(
							new JobPendingSlotRequestDetail.VertexTaskInfo(new JobVertexID(), "test_task", 21, 38)
					));

			final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
			final JsonNode marshalled = objectMapper.valueToTree(expected);
			final JobPendingSlotRequestDetail unmarshalled = objectMapper.treeToValue(marshalled, JobPendingSlotRequestDetail.class);

			assertEquals(expected, unmarshalled);
		}

		// case
		{
			Map<String, Resource> extendedResources = new HashMap<>();
			extendedResources.put(ResourceSpec.MANAGED_MEMORY_NAME, new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, 56));
			final JobPendingSlotRequestDetail expected = new JobPendingSlotRequestDetail(
					new SlotRequestId(),
					new ResourceProfile(0.123, 12, 23, 34, 45, extendedResources),
					625L,
					null,
					null,
					Lists.newArrayList(
							new JobPendingSlotRequestDetail.VertexTaskInfo(new JobVertexID(), "test_task", 21, 38)
					));

			final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
			final JsonNode marshalled = objectMapper.valueToTree(expected);
			final JobPendingSlotRequestDetail unmarshalled = objectMapper.treeToValue(marshalled, JobPendingSlotRequestDetail.class);

			assertEquals(expected, unmarshalled);
		}

		// case
		{
			final JobPendingSlotRequestDetail expected = new JobPendingSlotRequestDetail(
					new SlotRequestId(),
					new ResourceProfile(-1, -1, -1, -1, -1, null),
					625L,
					new SlotSharingGroupId(),
					new AbstractID(),
					Lists.newArrayList(
							new JobPendingSlotRequestDetail.VertexTaskInfo(new JobVertexID(), "test_task", 21, 38)
					));

			final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
			final JsonNode marshalled = objectMapper.valueToTree(expected);
			final JobPendingSlotRequestDetail unmarshalled = objectMapper.treeToValue(marshalled, JobPendingSlotRequestDetail.class);

			assertEquals(expected, unmarshalled);
		}
	}
}
