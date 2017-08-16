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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link HandlerResponse}.
 */
public class HandlerResponseTest {

	@Test
	public void testSuccessfulResponse() {
		TestResponse testResponse = new TestResponse();
		HandlerResponse<TestResponse> response = HandlerResponse.successful(testResponse);

		Assert.assertTrue(response.wasSuccessful());
		Assert.assertEquals(testResponse, response.getResponse());

		try {
			response.getErrorCode();
			Assert.fail();
		} catch (IllegalStateException expected) {
			// expected
		}
		try {
			response.getErrorMessage();
			Assert.fail();
		} catch (IllegalStateException expected) {
			// expected
		}
	}

	@Test
	public void testFailedResponse() {
		HandlerResponse<TestResponse> response = HandlerResponse.error("error", HttpResponseStatus.INTERNAL_SERVER_ERROR);

		Assert.assertFalse(response.wasSuccessful());
		Assert.assertEquals("error", response.getErrorMessage());
		Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.getErrorCode());

		try {
			response.getResponse();
			Assert.fail();
		} catch (IllegalStateException expected) {
			// expected
		}
	}

	private static class TestResponse implements ResponseBody {
	}
}
