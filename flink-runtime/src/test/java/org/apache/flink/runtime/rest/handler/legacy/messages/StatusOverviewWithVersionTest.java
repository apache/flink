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

package org.apache.flink.runtime.rest.handler.legacy.messages;

import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.TestLogger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link StatusOverviewWithVersion}.
 */
public class StatusOverviewWithVersionTest extends TestLogger {

	/**
	 * Tests that we can marshal and unmarshal StatusOverviewWithVersion.
	 */
	@Test
	public void testJsonMarshalling() throws JsonProcessingException {
		final StatusOverviewWithVersion expected = new StatusOverviewWithVersion(
			1,
			3,
			3,
			7,
			4,
			2,
			0,
			"version",
			"commit");

		ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

		JsonNode json = objectMapper.valueToTree(expected);

		final StatusOverviewWithVersion unmarshalled = objectMapper.treeToValue(json, StatusOverviewWithVersion.class);

		assertEquals(expected, unmarshalled);
	}
}
