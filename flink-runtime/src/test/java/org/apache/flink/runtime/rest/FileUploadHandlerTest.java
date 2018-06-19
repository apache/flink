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

import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link FileUploadHandler}. Ensures that multipart http messages containing files and/or json are properly
 * handled.
 */
public class FileUploadHandlerTest extends MultipartUploadTestBase {

	private static final ObjectMapper OBJECT_MAPPER = RestMapperUtils.getStrictObjectMapper();

	private static Request buildMalformedRequest(String headerUrl) {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addFilePart(builder);
		// this causes a failure in the FileUploadHandler since the request should only contain form-data
		builder = builder.addPart(okhttp3.RequestBody.create(MediaType.parse("text/plain"), "crash"));
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildMixedRequestWithUnknownAttribute(String headerUrl) throws IOException {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addJsonPart(builder, new TestRequestBody(), "hello");
		builder = addFilePart(builder);
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildFileRequest(String headerUrl) {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addFilePart(builder);
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildJsonRequest(String headerUrl, TestRequestBody json) throws IOException {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addJsonPart(builder, json, FileUploadHandler.HTTP_ATTRIBUTE_REQUEST);
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildMixedRequest(String headerUrl, TestRequestBody json) throws IOException {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addJsonPart(builder, json, FileUploadHandler.HTTP_ATTRIBUTE_REQUEST);
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

	private static MultipartBody.Builder addJsonPart(MultipartBody.Builder builder, TestRequestBody jsonRequestBody, String attribute) throws IOException {
		StringWriter sw = new StringWriter();
		OBJECT_MAPPER.writeValue(sw, jsonRequestBody);

		String jsonPayload = sw.toString();

		return builder.addFormDataPart(attribute, jsonPayload);
	}

	@Test
	public void testMixedMultipart() throws Exception {
		OkHttpClient client = new OkHttpClient();

		Request jsonRequest = buildJsonRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL(), new TestRequestBody());
		try (Response response = client.newCall(jsonRequest).execute()) {
			// explicitly rejected by the test handler implementation
			assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.code());
		}

		Request fileRequest = buildFileRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(fileRequest).execute()) {
			// expected JSON payload is missing
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}

		TestRequestBody json = new TestRequestBody();
		Request mixedRequest = buildMixedRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL(), json);
		try (Response response = client.newCall(mixedRequest).execute()) {
			assertEquals(mixedHandler.getMessageHeaders().getResponseStatusCode().code(), response.code());
			assertEquals(json, mixedHandler.lastReceivedRequest);
		}
	}

	@Test
	public void testJsonMultipart() throws Exception {
		OkHttpClient client = new OkHttpClient();

		TestRequestBody json = new TestRequestBody();
		Request jsonRequest = buildJsonRequest(jsonHandler.getMessageHeaders().getTargetRestEndpointURL(), json);
		try (Response response = client.newCall(jsonRequest).execute()) {
			assertEquals(jsonHandler.getMessageHeaders().getResponseStatusCode().code(), response.code());
			assertEquals(json, jsonHandler.lastReceivedRequest);
		}

		Request fileRequest = buildFileRequest(jsonHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(fileRequest).execute()) {
			// either because JSON payload is missing or FileUploads are outright forbidden
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}

		Request mixedRequest = buildMixedRequest(jsonHandler.getMessageHeaders().getTargetRestEndpointURL(), new TestRequestBody());
		try (Response response = client.newCall(mixedRequest).execute()) {
			// FileUploads are outright forbidden
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}
	}

	@Test
	public void testFileMultipart() throws Exception {
		OkHttpClient client = new OkHttpClient();

		Request jsonRequest = buildJsonRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL(), new TestRequestBody());
		try (Response response = client.newCall(jsonRequest).execute()) {
			// JSON payload did not match expected format
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}

		Request fileRequest = buildFileRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(fileRequest).execute()) {
			assertEquals(fileHandler.getMessageHeaders().getResponseStatusCode().code(), response.code());
		}

		Request mixedRequest = buildMixedRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL(), new TestRequestBody());
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
}
