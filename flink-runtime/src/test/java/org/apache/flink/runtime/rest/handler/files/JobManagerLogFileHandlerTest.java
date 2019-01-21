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

package org.apache.flink.runtime.rest.handler.files;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobManagerLogFileHeaders;
import org.apache.flink.runtime.rest.messages.LogFilenameQueryParameter;
import org.apache.flink.runtime.rest.messages.LogSizeQueryParameter;
import org.apache.flink.runtime.rest.messages.LogStartOffsetQueryParameter;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelProgressivePromise;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.util.Attribute;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.EventExecutor;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.ImmediateEventExecutor;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link JobManagerLogFileHandler}.
 */
public class JobManagerLogFileHandlerTest extends TestLogger {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testServingLogFile() throws IOException, HandlerRequestException, RestHandlerException {

		String fileContent = "This is a test";
		File logFile = createLogFile(fileContent);

		JobManagerLogFileHandler jobManagerLogFileHandler = new JobManagerLogFileHandler(
			() -> new CompletableFuture<>(),
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			JobManagerLogFileHeaders.getInstance(),
			logFile);

		int start = 0;
		int size = 15;
		File responseFile = temporaryFolder.newFile();
		ChannelHandlerContext ctx = new TestingChannelHandlerContext(responseFile);
		DefaultFullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/jobmanager/log");
		HandlerRequest handlerRequest = createHandlerRequest(start, size, null);

		jobManagerLogFileHandler.respondToRequest(ctx, httpRequest, handlerRequest, null);

		String result;
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(responseFile)))) {
			result = reader.readLine();
		}

		String expected = fileContent.substring(Math.min(start, fileContent.length()), Math.min(start + size, fileContent.length()));

		Assert.assertEquals(expected, result);

		Files.delete(responseFile.toPath());
		Files.delete(logFile.toPath());
	}

	private File createLogFile(String fileContent) throws IOException{
		File logDir = File.createTempFile("TestBaseUtils-logdir", null);
		assertTrue("Unable to delete temp file", logDir.delete());
		assertTrue("Unable to create temp directory", logDir.mkdir());
		File logFile = new File(logDir, "jobmanager.log");
		try (FileOutputStream fileOutputStream = new FileOutputStream(logFile)) {
			fileOutputStream.write(fileContent.getBytes());
		}
		return logFile;
	}

	private HandlerRequest createHandlerRequest(int start, int size, String filename) throws HandlerRequestException{
		Map<String, List<String>> queryParams = new HashMap<>();
		queryParams.put(new LogSizeQueryParameter().getKey(), Lists.newArrayList(String.valueOf(size)));
		queryParams.put(new LogStartOffsetQueryParameter().getKey(), Lists.newArrayList(String.valueOf(start)));
		queryParams.put(new LogFilenameQueryParameter().getKey(), filename == null ? Lists.newArrayList() : Lists.newArrayList(filename));

		HandlerRequest handlerRequest = new HandlerRequest(
			EmptyRequestBody.getInstance(),
			JobManagerLogFileHeaders.getInstance().getUnresolvedMessageParameters(),
			Collections.emptyMap(),
			queryParams);
		return handlerRequest;
	}

	/**
	 * Testing implementation of {@link ChannelHandlerContext}.
	 */
	private static final class TestingChannelHandlerContext implements ChannelHandlerContext {

		final File outputFile;

		private TestingChannelHandlerContext(File outputFile) {
			this.outputFile = Preconditions.checkNotNull(outputFile);
		}

		@Override
		public ChannelFuture write(Object msg, ChannelPromise promise) {
			if (msg instanceof DefaultFileRegion) {
				final DefaultFileRegion defaultFileRegion = (DefaultFileRegion) msg;

				try (final FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
					fileOutputStream.getChannel();

					defaultFileRegion.transferTo(fileOutputStream.getChannel(), 0L);
				} catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}
			}

			return new DefaultChannelPromise(new EmbeddedChannel());
		}

		@Override
		public EventExecutor executor() {
			return ImmediateEventExecutor.INSTANCE;
		}

		@Override
		public ChannelFuture write(Object msg) {
			return write(msg, null);
		}

		@Override
		public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
			final ChannelFuture channelFuture = write(msg, promise);
			flush();
			return channelFuture;
		}

		@Override
		public ChannelFuture writeAndFlush(Object msg) {
			return writeAndFlush(msg, null);
		}

		@Override
		public ChannelPipeline pipeline() {
			return mock(ChannelPipeline.class);
		}

		// -----------------------------------------------------
		// Automatically generated implementation
		// -----------------------------------------------------

		@Override
		public Channel channel() {
			return null;
		}

		@Override
		public String name() {
			return null;
		}

		@Override
		public ChannelHandler handler() {
			return null;
		}

		@Override
		public boolean isRemoved() {
			return false;
		}

		@Override
		public ChannelHandlerContext fireChannelRegistered() {
			return null;
		}

		@Override
		public ChannelHandlerContext fireChannelUnregistered() {
			return null;
		}

		@Override
		public ChannelHandlerContext fireChannelActive() {
			return null;
		}

		@Override
		public ChannelHandlerContext fireChannelInactive() {
			return null;
		}

		@Override
		public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
			return null;
		}

		@Override
		public ChannelHandlerContext fireUserEventTriggered(Object event) {
			return null;
		}

		@Override
		public ChannelHandlerContext fireChannelRead(Object msg) {
			return null;
		}

		@Override
		public ChannelHandlerContext fireChannelReadComplete() {
			return null;
		}

		@Override
		public ChannelHandlerContext fireChannelWritabilityChanged() {
			return null;
		}

		@Override
		public ChannelFuture bind(SocketAddress localAddress) {
			return null;
		}

		@Override
		public ChannelFuture connect(SocketAddress remoteAddress) {
			return null;
		}

		@Override
		public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
			return null;
		}

		@Override
		public ChannelFuture disconnect() {
			return null;
		}

		@Override
		public ChannelFuture close() {
			return null;
		}

		@Override
		public ChannelFuture deregister() {
			return null;
		}

		@Override
		public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
			return null;
		}

		@Override
		public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
			return null;
		}

		@Override
		public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
			return null;
		}

		@Override
		public ChannelFuture disconnect(ChannelPromise promise) {
			return null;
		}

		@Override
		public ChannelFuture close(ChannelPromise promise) {
			return null;
		}

		@Override
		public ChannelFuture deregister(ChannelPromise promise) {
			return null;
		}

		@Override
		public ChannelHandlerContext read() {
			return null;
		}

		@Override
		public ChannelHandlerContext flush() {
			return null;
		}

		@Override
		public ByteBufAllocator alloc() {
			return null;
		}

		@Override
		public ChannelPromise newPromise() {
			return null;
		}

		@Override
		public ChannelProgressivePromise newProgressivePromise() {
			return null;
		}

		@Override
		public ChannelFuture newSucceededFuture() {
			return null;
		}

		@Override
		public ChannelFuture newFailedFuture(Throwable cause) {
			return null;
		}

		@Override
		public ChannelPromise voidPromise() {
			return null;
		}

		@Override
		public <T> Attribute<T> attr(AttributeKey<T> key) {
			return null;
		}

		@Override
		public <T> boolean hasAttr(AttributeKey<T> attributeKey) {
			return false;
		}
	}

}
