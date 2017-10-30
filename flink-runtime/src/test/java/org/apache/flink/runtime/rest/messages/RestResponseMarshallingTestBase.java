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

import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test base for verifying that marshalling / unmarshalling REST {@link ResponseBody}s work properly.
 */
public abstract class RestResponseMarshallingTestBase<R extends ResponseBody> extends TestLogger {

	/**
	 * Returns the class of the test response.
	 *
	 * @return class of the test response type
	 */
	protected abstract Class<R> getTestResponseClass();

	/**
	 * Returns an instance of a response to be tested.
	 *
	 * @return instance of the expected test response
	 */
	protected abstract R getTestResponseInstance() throws Exception;

	/**
	 * Tests that we can marshal and unmarshal the response.
	 */
	@Test
	public void testJsonMarshalling() throws Exception {
		final R expected = getTestResponseInstance();

		ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
		JsonNode json = objectMapper.valueToTree(expected);

		final R unmarshalled = objectMapper.treeToValue(json, getTestResponseClass());
		Assert.assertEquals(expected, unmarshalled);
	}

}
