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

package org.apache.flink.runtime.rest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link FileUploadHandler}. Ensures that multipart http messages containing files and/or json are properly
 * handled.
 */
public class FileUploadHandlerTest extends TestLogger {

	private static final ObjectMapper OBJECT_MAPPER = RestMapperUtils.getStrictObjectMapper();
	private static final Random RANDOM = new Random();

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static RestServerEndpoint serverEndpoint;
	private static String serverAddress;

	private static MultipartMixedHandler mixedHandler;
	private static MultipartJsonHandler jsonHandler;
	private static MultipartFileHandler fileHandler;
	private static File file1;
	private static File file2;
	
	private static Path configuredUploadDir;

	@BeforeClass
	public static void setup() throws Exception {
		Configuration config = new Configuration();
		config.setInteger(RestOptions.PORT, 0);
		config.setString(RestOptions.ADDRESS, "localhost");
		configuredUploadDir = TEMPORARY_FOLDER.newFolder().toPath();
		config.setString(WebOptions.UPLOAD_DIR, configuredUploadDir.toString());

		RestServerEndpointConfiguration serverConfig = RestServerEndpointConfiguration.fromConfiguration(config);

		final String restAddress = "http://localhost:1234";
		RestfulGateway mockRestfulGateway = TestingRestfulGateway.newBuilder()
			.setRestAddress(restAddress)
			.build();

		final GatewayRetriever<RestfulGateway> mockGatewayRetriever = () ->
			CompletableFuture.completedFuture(mockRestfulGateway);

		file1 = TEMPORARY_FOLDER.newFile();
		Files.write(file1.toPath(), "hello".getBytes(ConfigConstants.DEFAULT_CHARSET));
		file2 = TEMPORARY_FOLDER.newFile();
		Files.write(file2.toPath(), "world".getBytes(ConfigConstants.DEFAULT_CHARSET));

		mixedHandler = new MultipartMixedHandler(CompletableFuture.completedFuture(restAddress), mockGatewayRetriever);
		jsonHandler = new MultipartJsonHandler(CompletableFuture.completedFuture(restAddress), mockGatewayRetriever);
		fileHandler = new MultipartFileHandler(CompletableFuture.completedFuture(restAddress), mockGatewayRetriever);

		final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = Arrays.asList(
			Tuple2.of(mixedHandler.getMessageHeaders(), mixedHandler),
			Tuple2.of(jsonHandler.getMessageHeaders(), jsonHandler),
			Tuple2.of(fileHandler.getMessageHeaders(), fileHandler));

		serverEndpoint = new TestRestServerEndpoint(serverConfig, handlers);

		serverEndpoint.start();
		serverAddress = serverEndpoint.getRestBaseUrl();
	}

	@AfterClass
	public static void teardown() throws Exception {
		if (serverEndpoint != null) {
			serverEndpoint.close();
			serverEndpoint = null;
		}
	}

	private static Request buildMalformedRequest(String headerUrl) {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addFilePart(builder);
		// this causes a failure in the FileUploadHandler since the request should only contain form-data
		builder = builder.addPart(okhttp3.RequestBody.create(MediaType.parse("text/plain"), "crash"));
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildMixedRequestWithUnknownAttribute(String headerUrl) throws IOException {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addJsonPart(builder, RANDOM.nextInt(), "hello");
		builder = addFilePart(builder);
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildFileRequest(String headerUrl) {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addFilePart(builder);
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildJsonRequest(String headerUrl, int index) throws IOException {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addJsonPart(builder, index, FileUploadHandler.HTTP_ATTRIBUTE_REQUEST);
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildMixedRequest(String headerUrl, int index) throws IOException {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addJsonPart(builder, index, FileUploadHandler.HTTP_ATTRIBUTE_REQUEST);
		builder = addFilePart(builder);
		return finalizeRequest(builder, headerUrl);
	}

	private static Request finalizeRequest(MultipartBody.Builder builder, String headerUrl) {
		MultipartBody multipartBody = builder
			.setType(MultipartBody.FORM)
			.build();

		return new Request.Builder()
			.url(serverAddress + headerUrl)
			.post(multipartBody)
			.build();
	}

	private static MultipartBody.Builder addFilePart(MultipartBody.Builder builder) {
		okhttp3.RequestBody filePayload1 = okhttp3.RequestBody.create(MediaType.parse("application/octet-stream"), file1);
		okhttp3.RequestBody filePayload2 = okhttp3.RequestBody.create(MediaType.parse("application/octet-stream"), file2);

		return builder.addFormDataPart("file1", file1.getName(), filePayload1)
			.addFormDataPart("file2", file2.getName(), filePayload2);
	}

	private static MultipartBody.Builder addJsonPart(MultipartBody.Builder builder, int index, String attribute) throws IOException {
		TestRequestBody jsonRequestBody = new TestRequestBody(index);

		StringWriter sw = new StringWriter();
		OBJECT_MAPPER.writeValue(sw, jsonRequestBody);

		String jsonPayload = sw.toString();

		return builder.addFormDataPart(attribute, jsonPayload);
	}

	@Test
	public void testMixedMultipart() throws Exception {
		OkHttpClient client = new OkHttpClient();

		Request jsonRequest = buildJsonRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL(), RANDOM.nextInt());
		try (Response response = client.newCall(jsonRequest).execute()) {
			// explicitly rejected by the test handler implementation
			assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.code());
		}

		Request fileRequest = buildFileRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(fileRequest).execute()) {
			// expected JSON payload is missing
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}

		int mixedId = RANDOM.nextInt();
		Request mixedRequest = buildMixedRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL(), mixedId);
		try (Response response = client.newCall(mixedRequest).execute()) {
			assertEquals(mixedHandler.getMessageHeaders().getResponseStatusCode().code(), response.code());
			assertEquals(mixedId, mixedHandler.lastReceivedRequest.index);
		}
	}

	@Test
	public void testJsonMultipart() throws Exception {
		OkHttpClient client = new OkHttpClient();

		int jsonId = RANDOM.nextInt();
		Request jsonRequest = buildJsonRequest(jsonHandler.getMessageHeaders().getTargetRestEndpointURL(), jsonId);
		try (Response response = client.newCall(jsonRequest).execute()) {
			assertEquals(jsonHandler.getMessageHeaders().getResponseStatusCode().code(), response.code());
			assertEquals(jsonId, jsonHandler.lastReceivedRequest.index);
		}

		Request fileRequest = buildFileRequest(jsonHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(fileRequest).execute()) {
			// either because JSON payload is missing or FileUploads are outright forbidden
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}

		Request mixedRequest = buildMixedRequest(jsonHandler.getMessageHeaders().getTargetRestEndpointURL(), RANDOM.nextInt());
		try (Response response = client.newCall(mixedRequest).execute()) {
			// FileUploads are outright forbidden
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}
	}

	@Test
	public void testFileMultipart() throws Exception {
		OkHttpClient client = new OkHttpClient();

		Request jsonRequest = buildJsonRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL(), RANDOM.nextInt());
		try (Response response = client.newCall(jsonRequest).execute()) {
			// JSON payload did not match expected format
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}

		Request fileRequest = buildFileRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(fileRequest).execute()) {
			assertEquals(fileHandler.getMessageHeaders().getResponseStatusCode().code(), response.code());
		}

		Request mixedRequest = buildMixedRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL(), RANDOM.nextInt());
		try (Response response = client.newCall(mixedRequest).execute()) {
			// JSON payload did not match expected format
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}
	}

	@Test
	public void testUploadCleanupOnUnknownAttribute() throws IOException {
		OkHttpClient client = new OkHttpClient();

		Request request = buildMixedRequestWithUnknownAttribute(mixedHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(request).execute()) {
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}
		assertUploadDirectoryIsEmpty();
	}

	/**
	 * Crashes the handler be submitting a malformed multipart request and tests that the upload directory is cleaned up.
	 */
	@Test
	public void testUploadCleanupOnFailure() throws IOException {
		OkHttpClient client = new OkHttpClient();

		Request request = buildMalformedRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(request).execute()) {
			// decoding errors aren't handled separately by the FileUploadHandler
			assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.code());
		}
		assertUploadDirectoryIsEmpty();
	}

	private static void assertUploadDirectoryIsEmpty() throws IOException {
		Preconditions.checkArgument(
			1 == Files.list(configuredUploadDir).count(),
			"Directory structure in rest upload directory has changed. Test must be adjusted");
		Optional<Path> actualUploadDir = Files.list(configuredUploadDir).findAny();
		Preconditions.checkArgument(
			actualUploadDir.isPresent(),
			"Expected upload directory does not exist.");
		assertEquals("Not all files were cleaned up.", 0, Files.list(actualUploadDir.get()).count());
	}

	private static class MultipartMixedHandler extends AbstractRestHandler<RestfulGateway, TestRequestBody, EmptyResponseBody, EmptyMessageParameters> {
		volatile TestRequestBody lastReceivedRequest = null;

		MultipartMixedHandler(CompletableFuture<String> localRestAddress, GatewayRetriever<RestfulGateway> leaderRetriever) {
			super(localRestAddress, leaderRetriever, RpcUtils.INF_TIMEOUT, Collections.emptyMap(), MultipartMixedHeaders.INSTANCE);
		}

		@Override
		protected CompletableFuture<EmptyResponseBody> handleRequest(@Nonnull HandlerRequest<TestRequestBody, EmptyMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
			MultipartFileHandler.verifyFileUpload(request.getUploadedFiles());
			this.lastReceivedRequest = request.getRequestBody();
			return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
		}

		private static final class MultipartMixedHeaders implements MessageHeaders<TestRequestBody, EmptyResponseBody, EmptyMessageParameters> {
			private static final MultipartMixedHeaders INSTANCE = new MultipartMixedHeaders();

			private MultipartMixedHeaders() {
			}

			@Override
			public Class<TestRequestBody> getRequestClass() {
				return TestRequestBody.class;
			}

			@Override
			public Class<EmptyResponseBody> getResponseClass() {
				return EmptyResponseBody.class;
			}

			@Override
			public HttpResponseStatus getResponseStatusCode() {
				return HttpResponseStatus.OK;
			}

			@Override
			public String getDescription() {
				return "";
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
				return "/test/upload/mixed";
			}

			@Override
			public boolean acceptsFileUploads() {
				return true;
			}
		}
	}

	private static class MultipartJsonHandler extends AbstractRestHandler<RestfulGateway, TestRequestBody, EmptyResponseBody, EmptyMessageParameters> {
		volatile TestRequestBody lastReceivedRequest = null;

		MultipartJsonHandler(CompletableFuture<String> localRestAddress, GatewayRetriever<RestfulGateway> leaderRetriever) {
			super(localRestAddress, leaderRetriever, RpcUtils.INF_TIMEOUT, Collections.emptyMap(), MultipartJsonHeaders.INSTANCE);
		}

		@Override
		protected CompletableFuture<EmptyResponseBody> handleRequest(@Nonnull HandlerRequest<TestRequestBody, EmptyMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
			Collection<Path> uploadedFiles = request.getUploadedFiles();
			if (!uploadedFiles.isEmpty()) {
				throw new RestHandlerException("This handler should not have received file uploads.", HttpResponseStatus.INTERNAL_SERVER_ERROR);
			}
			this.lastReceivedRequest = request.getRequestBody();
			return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
		}

		private static final class MultipartJsonHeaders extends TestHeadersBase<TestRequestBody> {
			private static final MultipartJsonHeaders INSTANCE = new MultipartJsonHeaders();

			private MultipartJsonHeaders() {
			}

			@Override
			public Class<TestRequestBody> getRequestClass() {
				return TestRequestBody.class;
			}

			@Override
			public String getTargetRestEndpointURL() {
				return "/test/upload/json";
			}

			@Override
			public boolean acceptsFileUploads() {
				return false;
			}
		}
	}

	private static class MultipartFileHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

		MultipartFileHandler(CompletableFuture<String> localRestAddress, GatewayRetriever<RestfulGateway> leaderRetriever) {
			super(localRestAddress, leaderRetriever, RpcUtils.INF_TIMEOUT, Collections.emptyMap(), MultipartFileHeaders.INSTANCE);
		}

		@Override
		protected CompletableFuture<EmptyResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
			verifyFileUpload(request.getUploadedFiles());
			return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
		}

		static void verifyFileUpload(Collection<Path> uploadedFiles) throws RestHandlerException {
			try {
				assertEquals(2, uploadedFiles.size());

				for (Path uploadedFile : uploadedFiles) {
					File matchingFile;
					if (uploadedFile.getFileName().toString().equals(file1.getName())) {
						matchingFile = file1;
					} else if (uploadedFile.getFileName().toString().equals(file2.getName())) {
						matchingFile = file2;
					} else {
						throw new RestHandlerException("Received file with unknown name " + uploadedFile.getFileName() + '.', HttpResponseStatus.INTERNAL_SERVER_ERROR);
					}

					byte[] originalContent = Files.readAllBytes(matchingFile.toPath());
					byte[] receivedContent = Files.readAllBytes(uploadedFile);
					assertArrayEquals(originalContent, receivedContent);
				}
			} catch (Exception e) {
				// return 505 to differentiate from common BAD_REQUEST responses in this test
				throw new RestHandlerException("Test verification failed.", HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
			}
		}

		private static final class MultipartFileHeaders extends TestHeadersBase<EmptyRequestBody> {
			private static final MultipartFileHeaders INSTANCE = new MultipartFileHeaders();

			private MultipartFileHeaders() {
			}

			@Override
			public Class<EmptyRequestBody> getRequestClass() {
				return EmptyRequestBody.class;
			}

			@Override
			public String getTargetRestEndpointURL() {
				return "/test/upload/file";
			}

			@Override
			public boolean acceptsFileUploads() {
				return true;
			}
		}
	}

	private abstract static class TestHeadersBase<R extends RequestBody> implements MessageHeaders<R, EmptyResponseBody, EmptyMessageParameters> {

		@Override
		public Class<EmptyResponseBody> getResponseClass() {
			return EmptyResponseBody.class;
		}

		@Override
		public HttpResponseStatus getResponseStatusCode() {
			return HttpResponseStatus.OK;
		}

		@Override
		public String getDescription() {
			return "";
		}

		@Override
		public EmptyMessageParameters getUnresolvedMessageParameters() {
			return EmptyMessageParameters.getInstance();
		}

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.POST;
		}
	}

	private static final class TestRequestBody implements RequestBody {
		private static final String FIELD_NAME_INDEX = "index";

		@JsonProperty(FIELD_NAME_INDEX)
		private final int index;

		@JsonCreator
		TestRequestBody(@JsonProperty(FIELD_NAME_INDEX) int index) {
			this.index = index;
		}
	}

	private static class TestRestServerEndpoint extends RestServerEndpoint {

		private final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers;

		TestRestServerEndpoint(
			RestServerEndpointConfiguration configuration,
			List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers) throws IOException {
			super(configuration);
			this.handlers = Preconditions.checkNotNull(handlers);
		}

		@Override
		protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
			return handlers;
		}

		@Override
		protected void startInternal() {
		}
	}
}
