/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.taskmanager.AbstractTaskManagerFileHandlerTest.TestingChannelHandlerContext;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.LogFileNamePathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.LogSearchInfoParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.LogSearchLineParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerLogSearchInfoHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerLogSearchInfoParameters;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


/**
 * Test for the {@link TaskManagerLogSearchInfoHandler}.
 */
public class TaskManagerLogSearchInfoHandlerTest extends TestLogger {
	private static final ResourceID EXPECTED_TASK_MANAGER_ID = ResourceID.generate();

	private static final DefaultFullHttpRequest HTTP_REQUEST = new DefaultFullHttpRequest(
		HttpVersion.HTTP_1_1, HttpMethod.GET, TestTaskManagerLogSearchInfoHeaders.URL);

	private static HandlerRequest<EmptyRequestBody, TaskManagerLogSearchInfoParameters> handlerRequest;

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static BlobServer blobServer;

	private static final String SEARCH_LINES = "0";
	private static final String SEARCH_WORD = "word";
	private static final String LOG_NAME = "search.log";
	private static final String RESULT_NAME = "result.log";

	private  File file1;
	private  File file2;
	private String fileContent1;
	private String fileContent2;
	private TransientBlobKey transientBlobKey;

	@BeforeClass
	public static void setup() throws IOException, HandlerRequestException {
		final Configuration configuration = new Configuration();
		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		blobServer = new BlobServer(configuration, new VoidBlobStore());

		handlerRequest = createRequest(EXPECTED_TASK_MANAGER_ID, LOG_NAME, SEARCH_WORD, SEARCH_LINES);
	}

	@Before
	public void setupTest() throws IOException {
		fileContent1 = "search word\n"
			+ "search";
		file1 = createFileWithContent(fileContent1, LOG_NAME);

		transientBlobKey = storeFileInBlobServer(file1);

		fileContent2 = "search word\n";
		file2 = createFileWithContent(fileContent2, RESULT_NAME);
	}

	@After
	public void delFileTest() {
		file1.deleteOnExit();
		file2.deleteOnExit();
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
	public void testFileSearching() throws Exception {
		final Time cacheEntryDuration = Time.milliseconds(1000L);

		final Queue<CompletableFuture<TransientBlobKey>> requestFileUploads = new ArrayDeque<>(1);

		requestFileUploads.add(CompletableFuture.completedFuture(transientBlobKey));

		final ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

		final TaskManagerLogSearchInfoHandler taskManagerLogSearchInfoHandler = new TaskManagerLogSearchInfoHandler(
			() -> CompletableFuture.completedFuture(null),
			TestingUtils.infiniteTime(),
			Collections.emptyMap(),
			new TestTaskManagerLogSearchInfoHeaders(),
			() -> CompletableFuture.completedFuture(resourceManagerGateway),
			blobServer,
			cacheEntryDuration
		) {
			@Override
			protected CompletableFuture<TransientBlobKey> requestFileUpload(ResourceManagerGateway resourceManagerGateway, Tuple2<ResourceID, String> taskManagerIdAndFileName) {
				final CompletableFuture<TransientBlobKey> transientBlobKeyFuture = requestFileUploads.poll();

				if (transientBlobKeyFuture != null) {
					return transientBlobKeyFuture;
				} else {
					return FutureUtils.completedExceptionally(new FlinkException("Could not upload file."));
				}
			}
		};

		final File outputFile = temporaryFolder.newFile();
		final TestingChannelHandlerContext testingContext = new AbstractTaskManagerFileHandlerTest.TestingChannelHandlerContext(outputFile);

		taskManagerLogSearchInfoHandler.respondToRequest(
			testingContext,
			HTTP_REQUEST,
			handlerRequest,
			null);

		assertThat(outputFile.length(), is(greaterThan(0L)));
		assertThat(FileUtils.readFileUtf8(outputFile), is(equalTo(fileContent2)));
	}

	private static TransientBlobKey storeFileInBlobServer(File fileToStore) throws IOException {
		// store the requested file in the BlobServer
		try (FileInputStream fileInputStream = new FileInputStream(fileToStore)) {
			return blobServer.getTransientBlobService().putTransient(fileInputStream);
		}
	}

	private static File createFileWithContent(String fileContent, String fileName) throws IOException {
		final File file = temporaryFolder.newFile(fileName);

		// write random content into the file
		try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
			fileOutputStream.write(fileContent.getBytes("UTF-8"));
		}

		return file;
	}

	private static HandlerRequest<EmptyRequestBody, TaskManagerLogSearchInfoParameters> createRequest(ResourceID taskManagerId, String fileName, String word, String lines) throws HandlerRequestException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put(TaskManagerIdPathParameter.KEY, taskManagerId.toString());
		parameters.put(LogFileNamePathParameter.KEY, fileName);
		parameters.put(LogSearchInfoParameter.KEY, word);
		parameters.put(LogSearchLineParameter.KEY, lines);
		Map<String, List<String>> queryParameters = Collections.emptyMap();

		return new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new TaskManagerLogSearchInfoParameters(),
			parameters,
			queryParameters);
	}


	/**
	 * Testing {@link TaskManagerLogSearchInfoHeaders}.
	 */
	private static final class TestTaskManagerLogSearchInfoHeaders implements
		UntypedResponseMessageHeaders<EmptyRequestBody, TaskManagerLogSearchInfoParameters> {

		private static final String URL = "/taskmanagers/:%s/logs/:%s/:%s/:%s";

		@Override
		public Class<EmptyRequestBody> getRequestClass() {
			return EmptyRequestBody.class;
		}

		@Override
		public TaskManagerLogSearchInfoParameters getUnresolvedMessageParameters() {
			return new TaskManagerLogSearchInfoParameters();
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
