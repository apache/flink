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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link MultipleJobsDetails} class.
 */
public class MultipleJobsDetailsTest extends TestLogger {

	/**
	 * Tests that we can un/marshal {@link MultipleJobsDetails} objects.
	 */
	@Test
	public void testMultipleJobsDetailsMarshalling() throws JsonProcessingException {
		int[] verticesPerState = new int[ExecutionState.values().length];

		for (int i = 0; i < verticesPerState.length; i++) {
			verticesPerState[i] = i;
		}

		final JobDetails running = new JobDetails(
			new JobID(),
			"running",
			1L,
			-1L,
			9L,
			JobStatus.RUNNING,
			9L,
			verticesPerState,
			9);

		final JobDetails finished = new JobDetails(
			new JobID(),
			"finished",
			1L,
			5L,
			4L,
			JobStatus.FINISHED,
			8L,
			verticesPerState,
			4);

		final MultipleJobsDetails expected = new MultipleJobsDetails(
			Arrays.asList(running, finished));

		final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

		final JsonNode marshalled = objectMapper.valueToTree(expected);

		final MultipleJobsDetails unmarshalled = objectMapper.treeToValue(marshalled, MultipleJobsDetails.class);

		assertEquals(expected, unmarshalled);
	}
}
