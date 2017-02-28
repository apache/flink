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
package org.apache.flink.runtime.webmonitor.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SubtaskExecutionAttemptAccumulatorsHandlerTest {
	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecution originalAttempt = ArchivedJobGenerationUtils.getTestAttempt();
		String json = SubtaskExecutionAttemptAccumulatorsHandler.createAttemptAccumulatorsJson(originalAttempt);

		JsonNode result = ArchivedJobGenerationUtils.mapper.readTree(json);

		assertEquals(1, result.get("subtask").asInt());
		assertEquals(3, result.get("attempt").asInt());
		assertEquals(originalAttempt.getAttemptId().toString(), result.get("id").asText());

		ArchivedJobGenerationUtils.compareStringifiedAccumulators(originalAttempt.getUserAccumulatorsStringified(), (ArrayNode) result.get("user-accumulators"));
	}
}
