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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link JobDetails}.
 */
public class JobDetailsTest extends TestLogger {

	/**
	 * Tests that we can marshal and unmarshal JobDetails instances.
	 */
	@Test
	public void testJobDetailsMarshalling() throws JsonProcessingException {
		final JobDetails expected = new JobDetails(
			new JobID(),
			"foobar",
			1L,
			10L,
			9L,
			JobStatus.RUNNING,
			8L,
			new int[]{1, 3, 3, 7, 4, 2, 7, 3, 3},
			42);

		final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

		final JsonNode marshalled = objectMapper.valueToTree(expected);

		final JobDetails unmarshalled = objectMapper.treeToValue(marshalled, JobDetails.class);

		assertEquals(expected, unmarshalled);
	}
}
