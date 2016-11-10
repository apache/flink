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
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.router.Handler;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests the RestServerProtocolHandler.
 */
public class RestServerProtocolHandlerTest {

	@Test
	public void testHandlerAdded() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new RestServerProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));
		assertNotNull(channel.pipeline().get(RestServerProtocolHandler.RestDecoder.class));
		assertNotNull(channel.pipeline().get(RestServerProtocolHandler.RestEncoder.class));
		assertTrue(channel.isActive());
	}

	// decoder ---------------------------------------------------

	@Test
	public void testDecodeRestRequestHeaderPassthru() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new Handler(TestUtils.router),
			new RestServerProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));

		DefaultFullHttpRequest request = new DefaultFullHttpRequest(
			HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
		request.headers().add(HttpHeaders.Names.CACHE_CONTROL, "private");
		channel.writeInbound(request);

		RestRequest decoded = (RestRequest) channel.inboundMessages().remove();
		assertEquals(0, request.refCnt());
		assertEquals(request.getMethod(), decoded.getMethod());
		assertEquals(request.getUri(), decoded.getUri());
		assertEquals("private", decoded.headers().get(HttpHeaders.Names.CACHE_CONTROL));
	}

	@Test
	public void testDecodeRestRequestContentDeserialization() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new Handler(TestUtils.router),
			new RestServerProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));

		TestUtils.TestEntity expectedContent = new TestUtils.TestEntity(1);
		DefaultFullHttpRequest request = new DefaultFullHttpRequest(
			HttpVersion.HTTP_1_1, HttpMethod.GET, "/", TestUtils.serialize(expectedContent, Charsets.UTF_8));
		request.headers().add(HttpHeaders.Names.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
		channel.writeInbound(request);

		RestRequest decoded = (RestRequest) channel.inboundMessages().remove();
		assertEquals(0, request.refCnt());
		assertEquals(expectedContent, decoded.content());
	}

	@Test(expected = DecoderException.class)
	public void testDecodeRestRequestInvalidContent() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new Handler(TestUtils.router),
			new RestServerProtocolHandler<>(TestUtils.mapper, TestUtils.UnreadableTestEntity.class));

		TestUtils.TestEntity badEntity = new TestUtils.TestEntity(1);
		DefaultFullHttpRequest request = new DefaultFullHttpRequest(
			HttpVersion.HTTP_1_1, HttpMethod.GET, "/", TestUtils.serialize(badEntity, Charsets.UTF_8));
		request.headers().add(HttpHeaders.Names.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
		channel.writeInbound(request);
		channel.checkException();
	}

	// encoder ---------------------------------------------------

	@Test
	public void testEncodeRestResponseHeaderPassthru() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new RestServerProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));

		DefaultRestResponse<Void> response = new DefaultRestResponse<>(
			HttpVersion.HTTP_1_1, HttpResponseStatus.OK, null);
		response.headers().add(HttpHeaders.Names.CACHE_CONTROL, "private");
		channel.writeOutbound(response);

		DefaultFullHttpResponse encoded = (DefaultFullHttpResponse) channel.outboundMessages().remove();
		assertEquals(1, encoded.refCnt());
		assertEquals(response.getStatus(), encoded.getStatus());
		assertEquals("private", encoded.headers().get(HttpHeaders.Names.CACHE_CONTROL));
		assertEquals(0, Integer.parseInt(encoded.headers().get(HttpHeaders.Names.CONTENT_LENGTH)));
		assertEquals(0, encoded.content().readableBytes());
	}

	@Test
	public void testEncodeRestResponseContentSerialization() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new RestServerProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));

		TestUtils.TestEntity expectedContent = new TestUtils.TestEntity(1);
		DefaultRestResponse<TestUtils.TestEntity> response = new DefaultRestResponse<>(
			HttpVersion.HTTP_1_1, HttpResponseStatus.OK, expectedContent);
		channel.writeOutbound(response);

		DefaultFullHttpResponse encoded = (DefaultFullHttpResponse) channel.outboundMessages().remove();
		assertEquals(1, encoded.refCnt());
		assertEquals(encoded.content().readableBytes(), Integer.parseInt(encoded.headers().get(HttpHeaders.Names.CONTENT_LENGTH)));
		assertEquals(MediaType.JSON_UTF_8, MediaType.parse(encoded.headers().get(HttpHeaders.Names.CONTENT_TYPE)));
		assertEquals(expectedContent, TestUtils.deserialize(encoded.content(), Charsets.UTF_8, TestUtils.TestEntity.class));
	}

	@Test(expected = EncoderException.class)
	public void testEncodeRestResponseInvalidContent() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new RestServerProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));

		DefaultRestResponse<TestUtils.UnwritableTestEntity> response = new DefaultRestResponse<>(
			HttpVersion.HTTP_1_1, HttpResponseStatus.OK, new TestUtils.UnwritableTestEntity());
		channel.writeOutbound(response);
		channel.checkException();
	}

	@Test
	public void testEncodeRestResponseErrorSerialization() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new RestServerProtocolHandler<>(TestUtils.mapper, TestUtils.TestEntity.class));

		RestError expectedError = new RestError(1, "error");
		DefaultRestResponse<TestUtils.TestEntity> response = new DefaultRestResponse<>(
			HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, null, true, expectedError);
		channel.writeOutbound(response);

		DefaultFullHttpResponse actual = (DefaultFullHttpResponse) channel.outboundMessages().remove();
		assertEquals(1, actual.refCnt());
		assertEquals(actual.content().readableBytes(), Integer.parseInt(actual.headers().get(HttpHeaders.Names.CONTENT_LENGTH)));
		assertEquals(MediaType.JSON_UTF_8, MediaType.parse(actual.headers().get(HttpHeaders.Names.CONTENT_TYPE)));
		assertEquals(expectedError, TestUtils.deserialize(actual.content(), Charsets.UTF_8, RestError.class));
	}
}
