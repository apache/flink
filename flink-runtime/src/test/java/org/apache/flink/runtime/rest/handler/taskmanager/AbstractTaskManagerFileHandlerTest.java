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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

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
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.util.Attribute;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.EventExecutor;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.ImmediateEventExecutor;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link AbstractTaskManagerFileHandler}.
 */
public class AbstractTaskManagerFileHandlerTest extends TestLogger {

	private static final ResourceID EXPECTED_TASK_MANAGER_ID = ResourceID.generate();

	private static final DefaultFullHttpRequest HTTP_REQUEST = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, TestUntypedMessageHeaders.URL);

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static BlobServer blobServer;

	private static HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> handlerRequest;

	private String fileContent1;

	private TransientBlobKey transientBlobKey1;

	private String fileContent2;

	private TransientBlobKey transientBlobKey2;

	@BeforeClass
	public static void setup() throws IOException, HandlerRequestException {
		final Configuration configuration = new Configuration();
		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		blobServer = new BlobServer(configuration, new VoidBlobStore());

		handlerRequest = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new TaskManagerMessageParameters(),
			Collections.singletonMap(TaskManagerIdPathParameter.KEY, EXPECTED_TASK_MANAGER_ID.getResourceIdString()),
			Collections.emptyMap());
	}

	@Before
	public void setupTest() throws IOException {
		fileContent1 = UUID.randomUUID().toString();
		final File file1 = createFileWithContent(fileContent1);
		transientBlobKey1 = storeFileInBlobServer(file1);

		fileContent2 = UUID.randomUUID().toString();
		final File file2 = createFileWithContent(fileContent2);
		transientBlobKey2 = storeFileInBlobServer(file2);
	}

	@AfterClass
	public static void teardown() throws IOException {
		if (blobServer != null) {
			blobServer.close();
			blobServer = null;
		}
	}

	/**
	 * Tests that the {@link AbstractTaskManagerFileHandler} serves the requested file.
	 */
	@Test
	public void testFileServing() throws Exception {
		final Time cacheEntryDuration = Time.milliseconds(1000L);

		final Queue<CompletableFuture<TransientBlobKey>> requestFileUploads = new ArrayDeque<>(1);

		requestFileUploads.add(CompletableFuture.completedFuture(transientBlobKey1));

		final TestTaskManagerFileHandler testTaskManagerFileHandler = createTestTaskManagerFileHandler(cacheEntryDuration, requestFileUploads, EXPECTED_TASK_MANAGER_ID);

		final File outputFile = temporaryFolder.newFile();
		final TestingChannelHandlerContext testingContext = new TestingChannelHandlerContext(outputFile);

		testTaskManagerFileHandler.respondToRequest(
			testingContext,
			HTTP_REQUEST,
			handlerRequest,
			null);

		assertThat(outputFile.length(), is(greaterThan(0L)));
		assertThat(FileUtils.readFileUtf8(outputFile), is(equalTo(fileContent1)));
	}

	/**
	 * Tests that files are cached.
	 */
	@Test
	public void testFileCaching() throws Exception {
		final File outputFile = runFileCachingTest(
			Time.milliseconds(5000L),
			Time.milliseconds(0L));

		assertThat(outputFile.length(), is(greaterThan(0L)));
		assertThat(FileUtils.readFileUtf8(outputFile), is(equalTo(fileContent1)));
	}

	/**
	 * Tests that file cache entries expire.
	 */
	@Test
	public void testFileCacheExpiration() throws Exception {
		final Time cacheEntryDuration = Time.milliseconds(5L);

		final File outputFile = runFileCachingTest(cacheEntryDuration, cacheEntryDuration);

		assertThat(outputFile.length(), is(greaterThan(0L)));
		assertThat(FileUtils.readFileUtf8(outputFile), is(equalTo(fileContent2)));
	}

	private File runFileCachingTest(
			Time cacheEntryDuration,
			Time delayBetweenRequests) throws Exception {
		final Queue<CompletableFuture<TransientBlobKey>> requestFileUploads = new ArrayDeque<>(2);
		requestFileUploads.add(CompletableFuture.completedFuture(transientBlobKey1));
		requestFileUploads.add(CompletableFuture.completedFuture(transientBlobKey2));

		final TestTaskManagerFileHandler testTaskManagerFileHandler = createTestTaskManagerFileHandler(
			cacheEntryDuration,
			requestFileUploads,
			EXPECTED_TASK_MANAGER_ID);

		final File outputFile = temporaryFolder.newFile();
		final TestingChannelHandlerContext testingContext = new TestingChannelHandlerContext(outputFile);

		testTaskManagerFileHandler.respondToRequest(
			testingContext,
			HTTP_REQUEST,
			handlerRequest,
			null);

		Thread.sleep(delayBetweenRequests.toMilliseconds());

		// the handler should not trigger the file upload again because it is still cached
		testTaskManagerFileHandler.respondToRequest(
			testingContext,
			HTTP_REQUEST,
			handlerRequest,
			null);
		return outputFile;
	}

	private AbstractTaskManagerFileHandlerTest.TestTaskManagerFileHandler createTestTaskManagerFileHandler(
		Time cacheEntryDuration,
		Queue<CompletableFuture<TransientBlobKey>> requestFileUploads,
		ResourceID expectedTaskManagerId) {
		final ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

		return new TestTaskManagerFileHandler(
			CompletableFuture.completedFuture("localhost"),
			() -> CompletableFuture.completedFuture(null),
			TestingUtils.infiniteTime(),
			Collections.emptyMap(),
			new TestUntypedMessageHeaders(),
			() -> CompletableFuture.completedFuture(resourceManagerGateway),
			blobServer,
			cacheEntryDuration,
			requestFileUploads,
			expectedTaskManagerId);
	}

	private static File createFileWithContent(String fileContent) throws IOException {
		final File file = temporaryFolder.newFile();

		// write random content into the file
		try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
			fileOutputStream.write(fileContent.getBytes("UTF-8"));
		}

		return file;
	}

	private static TransientBlobKey storeFileInBlobServer(File fileToStore) throws IOException {
		// store the requested file in the BlobServer
		try (FileInputStream fileInputStream = new FileInputStream(fileToStore)) {
			return blobServer.getTransientBlobService().putTransient(fileInputStream);
		}
	}

	/**
	 * Class under test.
	 */
	private static final class TestTaskManagerFileHandler extends AbstractTaskManagerFileHandler<TaskManagerMessageParameters> {

		private final Queue<CompletableFuture<TransientBlobKey>> requestFileUploads;

		private final ResourceID expectedTaskManagerId;

		protected TestTaskManagerFileHandler(@Nonnull CompletableFuture<String> localAddressFuture, @Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever, @Nonnull Time timeout, @Nonnull Map<String, String> responseHeaders, @Nonnull UntypedResponseMessageHeaders<EmptyRequestBody, TaskManagerMessageParameters> untypedResponseMessageHeaders, @Nonnull GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever, @Nonnull TransientBlobService transientBlobService, @Nonnull Time cacheEntryDuration, Queue<CompletableFuture<TransientBlobKey>> requestFileUploads, ResourceID expectedTaskManagerId) {
			super(localAddressFuture, leaderRetriever, timeout, responseHeaders, untypedResponseMessageHeaders, resourceManagerGatewayRetriever, transientBlobService, cacheEntryDuration);
			this.requestFileUploads = Preconditions.checkNotNull(requestFileUploads);
			this.expectedTaskManagerId = Preconditions.checkNotNull(expectedTaskManagerId);
		}

		@Override
		protected CompletableFuture<TransientBlobKey> requestFileUpload(ResourceManagerGateway resourceManagerGateway, ResourceID taskManagerResourceId) {
			assertThat(taskManagerResourceId, is(equalTo(expectedTaskManagerId)));
			final CompletableFuture<TransientBlobKey> transientBlobKeyFuture = requestFileUploads.poll();

			if (transientBlobKeyFuture != null) {
				return transientBlobKeyFuture;
			} else {
				return FutureUtils.completedExceptionally(new FlinkException("Could not upload file."));
			}
		}
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

			return new DefaultChannelPromise(null);
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
	}

	/**
	 * Testing {@link UntypedResponseMessageHeaders}.
	 */
	private static final class TestUntypedMessageHeaders implements UntypedResponseMessageHeaders<EmptyRequestBody, TaskManagerMessageParameters> {

		private static final String URL = "/foobar";

		@Override
		public Class<EmptyRequestBody> getRequestClass() {
			return EmptyRequestBody.class;
		}

		@Override
		public TaskManagerMessageParameters getUnresolvedMessageParameters() {
			return new TaskManagerMessageParameters();
		}

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.GET;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return URL;
		}
	}

}
