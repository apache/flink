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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Tests for {@link MessageParameters}.
 */
public class MessageParametersTest {
	@Test
	public void testResolveUrl() {
		String genericUrl = "/jobs/:jobid/state";
		TestMessageParameters parameters = new TestMessageParameters();
		parameters.pathParameter.resolve("1234");
		parameters.queryParameter.resolve("6789");

		String resolvedUrl = MessageParameters.resolveUrl(genericUrl, parameters);

		Assert.assertEquals("/jobs/1234/state?jobid=6789", resolvedUrl);
	}

	private static class TestMessageParameters extends MessageParameters {
		private final TestPathParameter pathParameter = new TestPathParameter();
		private final TestQueryParameter queryParameter = new TestQueryParameter();

		@Override
		public Collection<MessageParameter> getParameters() {
			return Collections.unmodifiableList(Arrays.asList(pathParameter, queryParameter));
		}
	}

	private static class TestPathParameter extends MessageParameter {

		TestPathParameter() {
			super("jobid", MessageParameterType.PATH, MessageParameterRequisiteness.MANDATORY);
		}
	}

	private static class TestQueryParameter extends MessageParameter {

		TestQueryParameter() {
			super("jobid", MessageParameterType.QUERY, MessageParameterRequisiteness.MANDATORY);
		}
	}
}
