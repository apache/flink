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

package org.apache.flink.runtime.rest.handler.util;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link HandlerRequestUtils}.
 */
public class HandlerRequestUtilsTest extends TestLogger {

	@Test
	public void testGetQueryParameter() throws Exception {
		final Boolean queryParameter = HandlerRequestUtils.getQueryParameter(
			new HandlerRequest<>(
				EmptyRequestBody.getInstance(),
				new TestMessageParameters(),
				Collections.emptyMap(),
				Collections.singletonMap("key", Collections.singletonList("true"))),
			TestBooleanQueryParameter.class);
		assertThat(queryParameter, equalTo(true));
	}

	@Test
	public void testGetQueryParameterRepeated() throws Exception {
		try {
			HandlerRequestUtils.getQueryParameter(
				new HandlerRequest<>(
					EmptyRequestBody.getInstance(),
					new TestMessageParameters(),
					Collections.emptyMap(),
					Collections.singletonMap("key", Arrays.asList("true", "false"))),
				TestBooleanQueryParameter.class);
		} catch (final RestHandlerException e) {
			assertThat(e.getMessage(), containsString("Expected only one value"));
		}
	}

	@Test
	public void testGetQueryParameterDefaultValue() throws Exception {
		final Boolean allowNonRestoredState = HandlerRequestUtils.getQueryParameter(
			new HandlerRequest<>(
				EmptyRequestBody.getInstance(),
				new TestMessageParameters(),
				Collections.emptyMap(),
				Collections.singletonMap("key", Collections.emptyList())),
			TestBooleanQueryParameter.class, true);
		assertThat(allowNonRestoredState, equalTo(true));
	}

	private static class TestMessageParameters extends MessageParameters {

		private TestBooleanQueryParameter testBooleanQueryParameter;

		@Override
		public Collection<MessagePathParameter<?>> getPathParameters() {
			return Collections.emptyList();
		}

		@Override
		public Collection<MessageQueryParameter<?>> getQueryParameters() {
			testBooleanQueryParameter = new TestBooleanQueryParameter();
			return Collections.singletonList(testBooleanQueryParameter);
		}
	}

	private static class TestBooleanQueryParameter extends MessageQueryParameter<Boolean> {

		private TestBooleanQueryParameter() {
			super("key", MessageParameterRequisiteness.OPTIONAL);
		}

		@Override
		public Boolean convertStringToValue(final String value) {
			return Boolean.parseBoolean(value);
		}

		@Override
		public String convertValueToString(final Boolean value) {
			return value.toString();
		}
	}
}
