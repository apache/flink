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

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.router.RouteResult;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.util.Attribute;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractHandler}.
 */
public class AbstractHandlerTest extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testFileCleanup() throws Exception {
		final Path dir = temporaryFolder.newFolder().toPath();
		final Path file = dir.resolve("file");
		Files.createFile(file);

		final String restAddress = "http://localhost:1234";
		RestfulGateway mockRestfulGateway = TestingRestfulGateway.newBuilder()
			.setRestAddress(restAddress)
			.build();

		final GatewayRetriever<RestfulGateway> mockGatewayRetriever = () ->
			CompletableFuture.completedFuture(mockRestfulGateway);

		CompletableFuture<Void> requestProcessingCompleteFuture = new CompletableFuture<>();
		TestHandler handler = new TestHandler(requestProcessingCompleteFuture, CompletableFuture.completedFuture(restAddress), mockGatewayRetriever);

		RouteResult<?> routeResult = new RouteResult<>("", "", Collections.emptyMap(), Collections.emptyMap(), "");
		HttpRequest request = new DefaultFullHttpRequest(
			HttpVersion.HTTP_1_1,
			HttpMethod.GET,
			TestHandler.TestHeaders.INSTANCE.getTargetRestEndpointURL(),
			Unpooled.wrappedBuffer(new byte[0]));
		RoutedRequest<?> routerRequest = new RoutedRequest<>(routeResult, request);

		Attribute<FileUploads> attribute = new SimpleAttribute();
		attribute.set(new FileUploads(dir));
		Channel channel = mock(Channel.class);
		when(channel.attr(any(AttributeKey.class))).thenReturn(attribute);

		ChannelHandlerContext context = mock(ChannelHandlerContext.class);
		when(context.channel()).thenReturn(channel);

		handler.respondAsLeader(context, routerRequest, mockRestfulGateway);

		// the (asynchronous) request processing is not yet complete so the files should still exist
		Assert.assertTrue(Files.exists(file));
		requestProcessingCompleteFuture.complete(null);
		Assert.assertFalse(Files.exists(file));
	}

	private static class SimpleAttribute implements Attribute<FileUploads> {

		private static final AttributeKey<FileUploads> KEY = AttributeKey.valueOf("test");

		private final AtomicReference<FileUploads> container = new AtomicReference<>();

		@Override
		public AttributeKey<FileUploads> key() {
			return KEY;
		}

		@Override
		public FileUploads get() {
			return container.get();
		}

		@Override
		public void set(FileUploads value) {
			container.set(value);
		}

		@Override
		public FileUploads getAndSet(FileUploads value) {
			return container.getAndSet(value);
		}

		@Override
		public FileUploads setIfAbsent(FileUploads value) {
			if (container.compareAndSet(null, value)) {
				return value;
			} else {
				return container.get();
			}
		}

		@Override
		public FileUploads getAndRemove() {
			return container.getAndSet(null);
		}

		@Override
		public boolean compareAndSet(FileUploads oldValue, FileUploads newValue) {
			return container.compareAndSet(oldValue, newValue);
		}

		@Override
		public void remove() {
			set(null);
		}
	}

	private static class TestHandler extends AbstractHandler<RestfulGateway, EmptyRequestBody, EmptyMessageParameters> {
		private final CompletableFuture<Void> completionFuture;

		protected TestHandler(CompletableFuture<Void> completionFuture, @Nonnull CompletableFuture<String> localAddressFuture, @Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever) {
			super(localAddressFuture, leaderRetriever, RpcUtils.INF_TIMEOUT, Collections.emptyMap(), TestHeaders.INSTANCE);
			this.completionFuture = completionFuture;
		}

		@Override
		protected CompletableFuture<Void> respondToRequest(ChannelHandlerContext ctx, HttpRequest httpRequest, HandlerRequest<EmptyRequestBody, EmptyMessageParameters> handlerRequest, RestfulGateway gateway) throws RestHandlerException {
			return completionFuture;
		}

		private enum TestHeaders implements UntypedResponseMessageHeaders<EmptyRequestBody, EmptyMessageParameters> {
			INSTANCE;

			@Override
			public Class<EmptyRequestBody> getRequestClass() {
				return EmptyRequestBody.class;
			}

			@Override
			public EmptyMessageParameters getUnresolvedMessageParameters() {
				return EmptyMessageParameters.getInstance();
			}

			@Override
			public HttpMethodWrapper getHttpMethod() {
				return HttpMethodWrapper.POST;
			}

			@Override
			public String getTargetRestEndpointURL() {
				return "/test";
			}

			@Override
			public boolean acceptsFileUploads() {
				return true;
			}
		}
	}
}
