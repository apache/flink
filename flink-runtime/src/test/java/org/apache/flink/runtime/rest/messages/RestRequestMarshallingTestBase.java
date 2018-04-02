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
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test base for verifying that marshalling / unmarshalling REST {@link RequestBody}s work properly.
 */
@Category(New.class)
public abstract class RestRequestMarshallingTestBase<R extends RequestBody> extends TestLogger {

	/**
	 * Returns the class of the test response.
	 *
	 * @return class of the test response type
	 */
	protected abstract Class<R> getTestRequestClass();

	/**
	 * Returns an instance of a response to be tested.
	 *
	 * @return instance of the expected test response
	 */
	protected abstract R getTestRequestInstance() throws Exception;

	/**
	 * Tests that we can marshal and unmarshal the response.
	 */
	@Test
	public void testJsonMarshalling() throws Exception {
		final R expected = getTestRequestInstance();

		ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
		final String marshalled = objectMapper.writeValueAsString(expected);

		final R unmarshalled = objectMapper.readValue(marshalled, getTestRequestClass());
		assertOriginalEqualsToUnmarshalled(expected, unmarshalled);
	}

	/**
	 * Asserts that two objects are equal. If they are not, an {@link AssertionError} is thrown.
	 *
	 * @param expected expected value
	 * @param actual   the value to check against expected
	 */
	protected void assertOriginalEqualsToUnmarshalled(R expected, R actual) {
		Assert.assertEquals(expected, actual);
	}

}
