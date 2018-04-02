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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link AbstractMetricsHeaders}.
 */
@Category(New.class)
public class AbstractMetricsHeadersTest extends TestLogger {

	private AbstractMetricsHeaders<EmptyMessageParameters> metricsHandlerHeaders;

	@Before
	public void setUp() throws Exception {
		metricsHandlerHeaders = new AbstractMetricsHeaders<EmptyMessageParameters>() {
			@Override
			public EmptyMessageParameters getUnresolvedMessageParameters() {
				return EmptyMessageParameters.getInstance();
			}

			@Override
			public String getTargetRestEndpointURL() {
				return "/";
			}
		};
	}

	@Test
	public void testHttpMethod() {
		assertThat(metricsHandlerHeaders.getHttpMethod(), equalTo(HttpMethodWrapper.GET));
	}

	@Test
	public void testResponseStatus() {
		assertThat(metricsHandlerHeaders.getResponseStatusCode(), equalTo(HttpResponseStatus.OK));
	}

	@Test
	public void testRequestClass() {
		assertThat(metricsHandlerHeaders.getRequestClass(), equalTo(EmptyRequestBody.class));
	}

	@Test
	public void testResponseClass() {
		assertThat(metricsHandlerHeaders.getResponseClass(), equalTo(MetricCollectionResponseBody.class));
	}

}
