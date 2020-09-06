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

package org.apache.flink.docs.rest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.docs.rest.data.TestEmptyMessageHeaders;
import org.apache.flink.docs.rest.data.TestExcludeMessageHeaders;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.util.DocumentingRestEndpoint;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link RestAPIDocGenerator}.
 */
public class RestAPIDocGeneratorTest extends TestLogger {

	@Test
	public void testExcludeFromDocumentation() throws Exception {
		File file = File.createTempFile("rest_v0_", ".html");
		RestAPIDocGenerator.createHtmlFile(
			new TestExcludeDocumentingRestEndpoint(),
			RestAPIVersion.V0,
			file.toPath());
		String actual = FileUtils.readFile(file, "UTF-8");

		assertThat(actual, containsString("/test/empty1"));
		assertThat(actual, containsString("This is a testing REST API."));
		assertThat(actual, containsString("/test/empty2"));
		assertThat(actual, containsString("This is another testing REST API."));
		assertThat(actual, not(containsString("/test/exclude1")));
		assertThat(actual, not(containsString("This REST API should not appear in the generated documentation.")));
		assertThat(actual, not(containsString("/test/exclude2")));
		assertThat(actual, not(containsString("This REST API should also not appear in the generated documentation.")));
	}

	private static class TestExcludeDocumentingRestEndpoint implements DocumentingRestEndpoint {

		@Override
		public List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> localAddressFuture) {
			return Arrays.asList(
				Tuple2.of(
					new TestEmptyMessageHeaders(
						"/test/empty1",
						"This is a testing REST API."), null),
				Tuple2.of(
					new TestEmptyMessageHeaders(
						"/test/empty2",
						"This is another testing REST API."), null),
				Tuple2.of(
					new TestExcludeMessageHeaders(
						"/test/exclude1",
						"This REST API should not appear in the generated documentation."), null),
				Tuple2.of(
					new TestExcludeMessageHeaders(
						"/test/exclude2",
						"This REST API should also not appear in the generated documentation."), null));
		}
	}
}
