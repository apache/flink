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
 * Tests for the {@link DashboardConfiguration}.
 */
public class DashboardConfigurationTest extends TestLogger {

	/**
	 * Tests that we can marshal and unmarshal {@link DashboardConfiguration} objects.
	 */
	@Test
	public void testJsonMarshalling() throws JsonProcessingException {
		final DashboardConfiguration expected = new DashboardConfiguration(
			1L,
			"foobar",
			42,
			"version",
			"revision");

		final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

		JsonNode marshaled = objectMapper.valueToTree(expected);

		final DashboardConfiguration unmarshaled = objectMapper.treeToValue(marshaled, DashboardConfiguration.class);

		assertEquals(expected, unmarshaled);
	}
}
