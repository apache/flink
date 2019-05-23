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

import org.apache.flink.runtime.io.network.netty.NettyLeakDetectionResource;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link FileUploadHandler}. Ensures that multipart http messages containing files and/or json are properly
 * handled.
 */
public class FileUploadHandlerTest extends TestLogger {

	@ClassRule
	public static final MultipartUploadResource MULTIPART_UPLOAD_RESOURCE = new MultipartUploadResource();

	private static final ObjectMapper OBJECT_MAPPER = RestMapperUtils.getStrictObjectMapper();

	@ClassRule
	public static final NettyLeakDetectionResource LEAK_DETECTION = new NettyLeakDetectionResource();

	@After
	public void reset() {
		MULTIPART_UPLOAD_RESOURCE.resetState();
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
		builder = addJsonPart(builder, new MultipartUploadResource.TestRequestBody(), "hello");
		builder = addFilePart(builder);
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildFileRequest(String headerUrl) {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addFilePart(builder);
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildJsonRequest(String headerUrl, MultipartUploadResource.TestRequestBody json) throws IOException {
		MultipartBody.Builder builder = new MultipartBody.Builder();
		builder = addJsonPart(builder, json, FileUploadHandler.HTTP_ATTRIBUTE_REQUEST);
		return finalizeRequest(builder, headerUrl);
	}

	private static Request buildMixedRequest(String headerUrl, MultipartUploadResource.TestRequestBody json) throws IOException {
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
			.url(MULTIPART_UPLOAD_RESOURCE.serverAddress + headerUrl)
			.post(multipartBody)
			.build();
	}

	private static MultipartBody.Builder addFilePart(MultipartBody.Builder builder) {
		for (File file : MULTIPART_UPLOAD_RESOURCE.getFilesToUpload()) {
			okhttp3.RequestBody filePayload = okhttp3.RequestBody.create(MediaType.parse("application/octet-stream"), file);

			builder = builder.addFormDataPart(file.getName(), file.getName(), filePayload);
		}

		return builder;
	}

	private static MultipartBody.Builder addJsonPart(MultipartBody.Builder builder, MultipartUploadResource.TestRequestBody jsonRequestBody, String attribute) throws IOException {
		StringWriter sw = new StringWriter();
		OBJECT_MAPPER.writeValue(sw, jsonRequestBody);

		String jsonPayload = sw.toString();

		return builder.addFormDataPart(attribute, jsonPayload);
	}

	@Test
	public void testUploadDirectoryRegeneration() throws Exception {
		OkHttpClient client = new OkHttpClient();

		MultipartUploadResource.MultipartFileHandler fileHandler = MULTIPART_UPLOAD_RESOURCE.getFileHandler();

		FileUtils.deleteDirectory(MULTIPART_UPLOAD_RESOURCE.getUploadDirectory().toFile());

		Request fileRequest = buildFileRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(fileRequest).execute()) {
			assertEquals(fileHandler.getMessageHeaders().getResponseStatusCode().code(), response.code());
		}
	}

	@Test
	public void testMixedMultipart() throws Exception {
		OkHttpClient client = new OkHttpClient();

		MultipartUploadResource.MultipartMixedHandler mixedHandler = MULTIPART_UPLOAD_RESOURCE.getMixedHandler();

		Request jsonRequest = buildJsonRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL(), new MultipartUploadResource.TestRequestBody());
		try (Response response = client.newCall(jsonRequest).execute()) {
			// explicitly rejected by the test handler implementation
			assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.code());
		}

		Request fileRequest = buildFileRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(fileRequest).execute()) {
			// expected JSON payload is missing
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}

		MultipartUploadResource.TestRequestBody json = new MultipartUploadResource.TestRequestBody();
		Request mixedRequest = buildMixedRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL(), json);
		try (Response response = client.newCall(mixedRequest).execute()) {
			assertEquals(mixedHandler.getMessageHeaders().getResponseStatusCode().code(), response.code());
			assertEquals(json, mixedHandler.lastReceivedRequest);
		}
	}

	@Test
	public void testJsonMultipart() throws Exception {
		OkHttpClient client = new OkHttpClient();

		MultipartUploadResource.MultipartJsonHandler jsonHandler = MULTIPART_UPLOAD_RESOURCE.getJsonHandler();

		MultipartUploadResource.TestRequestBody json = new MultipartUploadResource.TestRequestBody();
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

		Request mixedRequest = buildMixedRequest(jsonHandler.getMessageHeaders().getTargetRestEndpointURL(), new MultipartUploadResource.TestRequestBody());
		try (Response response = client.newCall(mixedRequest).execute()) {
			// FileUploads are outright forbidden
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}
	}

	@Test
	public void testFileMultipart() throws Exception {
		OkHttpClient client = new OkHttpClient();

		MultipartUploadResource.MultipartFileHandler fileHandler = MULTIPART_UPLOAD_RESOURCE.getFileHandler();

		Request jsonRequest = buildJsonRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL(), new MultipartUploadResource.TestRequestBody());
		try (Response response = client.newCall(jsonRequest).execute()) {
			// JSON payload did not match expected format
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}

		Request fileRequest = buildFileRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(fileRequest).execute()) {
			assertEquals(fileHandler.getMessageHeaders().getResponseStatusCode().code(), response.code());
		}

		Request mixedRequest = buildMixedRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL(), new MultipartUploadResource.TestRequestBody());
		try (Response response = client.newCall(mixedRequest).execute()) {
			// JSON payload did not match expected format
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}
	}

	@Test
	public void testUploadCleanupOnUnknownAttribute() throws IOException {
		OkHttpClient client = new OkHttpClient();

		Request request = buildMixedRequestWithUnknownAttribute(MULTIPART_UPLOAD_RESOURCE.getMixedHandler().getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(request).execute()) {
			assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code());
		}
		MULTIPART_UPLOAD_RESOURCE.assertUploadDirectoryIsEmpty();
	}

	/**
	 * Crashes the handler be submitting a malformed multipart request and tests that the upload directory is cleaned up.
	 */
	@Test
	public void testUploadCleanupOnFailure() throws IOException {
		OkHttpClient client = new OkHttpClient();

		Request request = buildMalformedRequest(MULTIPART_UPLOAD_RESOURCE.getMixedHandler().getMessageHeaders().getTargetRestEndpointURL());
		try (Response response = client.newCall(request).execute()) {
			// decoding errors aren't handled separately by the FileUploadHandler
			assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.code());
		}
		MULTIPART_UPLOAD_RESOURCE.assertUploadDirectoryIsEmpty();
	}
}
