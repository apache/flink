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

package org.apache.flink.runtime.net.http;

import com.google.common.base.Charsets;
import com.google.common.net.MediaType;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests the {@link RestClientProtocolHandler}.
 */
public class RestClientProtocolHandlerTest {

	@Test
	public void testHandlerAdded() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new RestClientProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));
		assertNotNull(channel.pipeline().get(RestClientProtocolHandler.RestDecoder.class));
		assertNotNull(channel.pipeline().get(RestClientProtocolHandler.RestEncoder.class));
		assertTrue(channel.isActive());
	}

	// encoder ---------------------------------------------------

	@Test
	public void testEncodeRestRequestContentSerialization() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new RestClientProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));

		TestUtils.TestEntity expectedContent = new TestUtils.TestEntity(1);
		DefaultRestRequest<TestUtils.TestEntity> request = new DefaultRestRequest<>(
			HttpVersion.HTTP_1_1, HttpMethod.GET, "/", expectedContent);
		channel.writeOutbound(request);

		FullHttpRequest encoded = (FullHttpRequest) channel.outboundMessages().remove();
		assertEquals(encoded.content().readableBytes(), Integer.parseInt(encoded.headers().get(HttpHeaders.Names.CONTENT_LENGTH)));
		assertEquals(MediaType.JSON_UTF_8, MediaType.parse(encoded.headers().get(HttpHeaders.Names.CONTENT_TYPE)));
		assertEquals(expectedContent, TestUtils.deserialize(encoded.content(), Charsets.UTF_8, TestUtils.TestEntity.class));
	}

	// decoder ---------------------------------------------------

	@Test
	public void testDecodeRestResponseContentDeserialization() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new RestClientProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));

		TestUtils.TestEntity expectedContent = new TestUtils.TestEntity(1);
		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
			HttpVersion.HTTP_1_1, HttpResponseStatus.OK, TestUtils.serialize(expectedContent, Charsets.UTF_8));
		response.headers().add(HttpHeaders.Names.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
		channel.writeInbound(response);

		RestResponse<TestUtils.TestEntity> decoded =
			(RestResponse<TestUtils.TestEntity>) channel.inboundMessages().remove();
		assertEquals(expectedContent, decoded.content());
	}

	@Test
	public void testDecodeRestResponseErrorDeserialization() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new RestClientProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));

		RestError expectedError = new RestError(1, "error");
		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
			HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR,
			TestUtils.serialize(expectedError, Charsets.UTF_8));
		response.headers().add(HttpHeaders.Names.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
		channel.writeInbound(response);

		RestResponse<TestUtils.TestEntity> decoded =
			(RestResponse<TestUtils.TestEntity>) channel.inboundMessages().remove();
		assertNull(decoded.content());
		assertEquals(expectedError, decoded.error());
	}
}
