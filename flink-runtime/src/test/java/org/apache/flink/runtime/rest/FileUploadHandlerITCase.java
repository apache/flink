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

import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.io.network.netty.NettyLeakDetectionExtension;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.BiConsumerWithException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for the {@link FileUploadHandler}. Ensures that multipart http messages containing files
 * and/or json are properly handled.
 */
class FileUploadHandlerITCase {

    @TempDir public Path tempDir;

    @RegisterExtension
    public final EachCallbackWrapper<MultipartUploadExtension> multipartUpdateExtensionWrapper =
            new EachCallbackWrapper<>(new MultipartUploadExtension(() -> tempDir));

    private static final ObjectMapper OBJECT_MAPPER = RestMapperUtils.getStrictObjectMapper();

    @RegisterExtension
    public static final NettyLeakDetectionExtension LEAK_DETECTION =
            new NettyLeakDetectionExtension();

    private Request buildMalformedRequest(String headerUrl) {
        MultipartBody.Builder builder = new MultipartBody.Builder();
        builder = addFilePart(builder);
        // this causes a failure in the FileUploadHandler since the request should only contain
        // form-data
        builder =
                builder.addPart(okhttp3.RequestBody.create(MediaType.parse("text/plain"), "crash"));
        return finalizeRequest(builder, headerUrl);
    }

    private Request buildMixedRequestWithUnknownAttribute(String headerUrl) throws IOException {
        MultipartBody.Builder builder = new MultipartBody.Builder();
        builder = addJsonPart(builder, new MultipartUploadExtension.TestRequestBody(), "hello");
        builder = addFilePart(builder);
        return finalizeRequest(builder, headerUrl);
    }

    private Request buildRequestWithCustomFilenames(
            String headerUrl, String filename1, String filename2) {
        MultipartBody.Builder builder = new MultipartBody.Builder();
        builder =
                addFilePart(
                        builder,
                        multipartUpdateExtensionWrapper.getCustomExtension().file1,
                        filename1);
        builder =
                addFilePart(
                        builder,
                        multipartUpdateExtensionWrapper.getCustomExtension().file2,
                        filename2);
        return finalizeRequest(builder, headerUrl);
    }

    private Request buildFileRequest(String headerUrl) {
        MultipartBody.Builder builder = new MultipartBody.Builder();
        builder = addFilePart(builder);
        return finalizeRequest(builder, headerUrl);
    }

    private Request buildJsonRequest(
            String headerUrl, MultipartUploadExtension.TestRequestBody json) throws IOException {
        MultipartBody.Builder builder = new MultipartBody.Builder();
        builder = addJsonPart(builder, json, FileUploadHandler.HTTP_ATTRIBUTE_REQUEST);
        return finalizeRequest(builder, headerUrl);
    }

    private Request buildMixedRequest(
            String headerUrl, MultipartUploadExtension.TestRequestBody json, File file)
            throws IOException {
        MultipartBody.Builder builder = new MultipartBody.Builder();
        builder = addJsonPart(builder, json, FileUploadHandler.HTTP_ATTRIBUTE_REQUEST);
        builder = addFilePart(builder, file, file.getName());
        return finalizeRequest(builder, headerUrl);
    }

    private Request buildMixedRequest(
            String headerUrl, MultipartUploadExtension.TestRequestBody json) throws IOException {
        MultipartBody.Builder builder = new MultipartBody.Builder();
        builder = addJsonPart(builder, json, FileUploadHandler.HTTP_ATTRIBUTE_REQUEST);
        builder = addFilePart(builder);
        return finalizeRequest(builder, headerUrl);
    }

    private Request finalizeRequest(MultipartBody.Builder builder, String headerUrl) {
        MultipartBody multipartBody = builder.setType(MultipartBody.FORM).build();

        return new Request.Builder()
                .url(multipartUpdateExtensionWrapper.getCustomExtension().serverAddress + headerUrl)
                .post(multipartBody)
                .build();
    }

    private MultipartBody.Builder addFilePart(final MultipartBody.Builder builder) {
        multipartUpdateExtensionWrapper
                .getCustomExtension()
                .getFilesToUpload()
                .forEach(f -> addFilePart(builder, f, f.getName()));
        return builder;
    }

    private static MultipartBody.Builder addFilePart(
            MultipartBody.Builder builder, File file, String filename) {
        okhttp3.RequestBody filePayload =
                okhttp3.RequestBody.create(MediaType.parse("application/octet-stream"), file);
        builder = builder.addFormDataPart(file.getName(), filename, filePayload);

        return builder;
    }

    private static MultipartBody.Builder addJsonPart(
            MultipartBody.Builder builder,
            MultipartUploadExtension.TestRequestBody jsonRequestBody,
            String attribute)
            throws IOException {
        StringWriter sw = new StringWriter();
        OBJECT_MAPPER.writeValue(sw, jsonRequestBody);

        String jsonPayload = sw.toString();

        return builder.addFormDataPart(attribute, jsonPayload);
    }

    @Test
    void testUploadDirectoryRegeneration() throws Exception {
        OkHttpClient client = createOkHttpClientWithNoTimeouts();

        MultipartUploadExtension.MultipartFileHandler fileHandler =
                multipartUpdateExtensionWrapper.getCustomExtension().getFileHandler();

        FileUtils.deleteDirectory(
                multipartUpdateExtensionWrapper.getCustomExtension().getUploadDirectory().toFile());

        Request fileRequest =
                buildFileRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL());
        try (Response response = client.newCall(fileRequest).execute()) {
            assertThat(response.code())
                    .isEqualTo(fileHandler.getMessageHeaders().getResponseStatusCode().code());
        }

        verifyNoFileIsRegisteredToDeleteOnExitHook();
    }

    @Test
    void testMixedMultipart() throws Exception {
        OkHttpClient client = createOkHttpClientWithNoTimeouts();

        MultipartUploadExtension.MultipartMixedHandler mixedHandler =
                multipartUpdateExtensionWrapper.getCustomExtension().getMixedHandler();

        Request jsonRequest =
                buildJsonRequest(
                        mixedHandler.getMessageHeaders().getTargetRestEndpointURL(),
                        new MultipartUploadExtension.TestRequestBody());
        try (Response response = client.newCall(jsonRequest).execute()) {
            // explicitly rejected by the test handler implementation
            assertThat(response.code()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
        }

        Request fileRequest =
                buildFileRequest(mixedHandler.getMessageHeaders().getTargetRestEndpointURL());
        try (Response response = client.newCall(fileRequest).execute()) {
            // expected JSON payload is missing
            assertThat(response.code()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        }

        MultipartUploadExtension.TestRequestBody json =
                new MultipartUploadExtension.TestRequestBody();
        Request mixedRequest =
                buildMixedRequest(
                        mixedHandler.getMessageHeaders().getTargetRestEndpointURL(), json);
        try (Response response = client.newCall(mixedRequest).execute()) {
            assertThat(response.code())
                    .isEqualTo(mixedHandler.getMessageHeaders().getResponseStatusCode().code());
            assertThat(mixedHandler.lastReceivedRequest).isEqualTo(json);
        }

        verifyNoFileIsRegisteredToDeleteOnExitHook();
    }

    /**
     * This test checks for a specific multipart request chunk layout using a magic number.
     *
     * <p>These things are very susceptible to interference from other requests or parts of the
     * payload; for example if the JSON payload increases by a single byte it can already break the
     * number. Do not reuse the client.
     *
     * <p>To find the magic number you can define a static counter, and loop the test in the IDE
     * (without forking!) while incrementing the counter on each run.
     */
    @Test
    void testMixedMultipartEndOfDataDecoderExceptionHandling(@TempDir Path tmp) throws Exception {
        OkHttpClient client = createOkHttpClientWithNoTimeouts();

        MultipartUploadExtension.MultipartMixedHandler mixedHandler =
                multipartUpdateExtensionWrapper.getCustomExtension().getMixedHandler();

        MultipartUploadExtension.TestRequestBody json =
                new MultipartUploadExtension.TestRequestBody();

        File file = TempDirUtils.newFile(tmp);
        try (RandomAccessFile rw = new RandomAccessFile(file, "rw")) {
            // magic value that reliably reproduced EndOfDataDecoderException in hasNext()
            rw.setLength(1424);
        }
        multipartUpdateExtensionWrapper
                .getCustomExtension()
                .setFileUploadVerifier(
                        (handlerRequest, restfulGateway) ->
                                MultipartUploadExtension.assertUploadedFilesEqual(
                                        handlerRequest, Collections.singleton(file)));

        Request singleFileMixedRequest =
                buildMixedRequest(
                        mixedHandler.getMessageHeaders().getTargetRestEndpointURL(), json, file);
        try (Response response = client.newCall(singleFileMixedRequest).execute()) {
            assertThat(response.code())
                    .isEqualTo(mixedHandler.getMessageHeaders().getResponseStatusCode().code());
            assertThat(mixedHandler.lastReceivedRequest).isEqualTo(json);
        }

        verifyNoFileIsRegisteredToDeleteOnExitHook();
    }

    @Test
    void testJsonMultipart() throws Exception {
        OkHttpClient client = createOkHttpClientWithNoTimeouts();

        MultipartUploadExtension.MultipartJsonHandler jsonHandler =
                multipartUpdateExtensionWrapper.getCustomExtension().getJsonHandler();

        MultipartUploadExtension.TestRequestBody json =
                new MultipartUploadExtension.TestRequestBody();
        Request jsonRequest =
                buildJsonRequest(jsonHandler.getMessageHeaders().getTargetRestEndpointURL(), json);
        try (Response response = client.newCall(jsonRequest).execute()) {
            assertThat(response.code())
                    .isEqualTo(jsonHandler.getMessageHeaders().getResponseStatusCode().code());
            assertThat(jsonHandler.lastReceivedRequest).isEqualTo(json);
        }

        Request fileRequest =
                buildFileRequest(jsonHandler.getMessageHeaders().getTargetRestEndpointURL());
        try (Response response = client.newCall(fileRequest).execute()) {
            // either because JSON payload is missing or FileUploads are outright forbidden
            assertThat(response.code()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        }

        Request mixedRequest =
                buildMixedRequest(
                        jsonHandler.getMessageHeaders().getTargetRestEndpointURL(),
                        new MultipartUploadExtension.TestRequestBody());
        try (Response response = client.newCall(mixedRequest).execute()) {
            // FileUploads are outright forbidden
            assertThat(response.code()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        }

        verifyNoFileIsRegisteredToDeleteOnExitHook();
    }

    @Test
    void testFileMultipart() throws Exception {
        OkHttpClient client = createOkHttpClientWithNoTimeouts();

        MultipartUploadExtension.MultipartFileHandler fileHandler =
                multipartUpdateExtensionWrapper.getCustomExtension().getFileHandler();

        Request jsonRequest =
                buildJsonRequest(
                        fileHandler.getMessageHeaders().getTargetRestEndpointURL(),
                        new MultipartUploadExtension.TestRequestBody());
        try (Response response = client.newCall(jsonRequest).execute()) {
            // JSON payload did not match expected format
            assertThat(response.code()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        }

        Request fileRequest =
                buildFileRequest(fileHandler.getMessageHeaders().getTargetRestEndpointURL());
        try (Response response = client.newCall(fileRequest).execute()) {
            assertThat(response.code())
                    .isEqualTo(fileHandler.getMessageHeaders().getResponseStatusCode().code());
        }

        Request mixedRequest =
                buildMixedRequest(
                        fileHandler.getMessageHeaders().getTargetRestEndpointURL(),
                        new MultipartUploadExtension.TestRequestBody());
        try (Response response = client.newCall(mixedRequest).execute()) {
            // JSON payload did not match expected format
            assertThat(response.code()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        }

        verifyNoFileIsRegisteredToDeleteOnExitHook();
    }

    @Test
    void testUploadCleanupOnUnknownAttribute() throws IOException {
        OkHttpClient client = createOkHttpClientWithNoTimeouts();

        Request request =
                buildMixedRequestWithUnknownAttribute(
                        multipartUpdateExtensionWrapper
                                .getCustomExtension()
                                .getMixedHandler()
                                .getMessageHeaders()
                                .getTargetRestEndpointURL());
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        }
        multipartUpdateExtensionWrapper.getCustomExtension().assertUploadDirectoryIsEmpty();

        verifyNoFileIsRegisteredToDeleteOnExitHook();
    }

    /**
     * Crashes the handler be submitting a malformed multipart request and tests that the upload
     * directory is cleaned up.
     */
    @Test
    void testUploadCleanupOnFailure() throws IOException {
        OkHttpClient client = createOkHttpClientWithNoTimeouts();

        Request request =
                buildMalformedRequest(
                        multipartUpdateExtensionWrapper
                                .getCustomExtension()
                                .getMixedHandler()
                                .getMessageHeaders()
                                .getTargetRestEndpointURL());
        try (Response response = client.newCall(request).execute()) {
            // decoding errors aren't handled separately by the FileUploadHandler
            assertThat(response.code()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
        }
        multipartUpdateExtensionWrapper.getCustomExtension().assertUploadDirectoryIsEmpty();

        verifyNoFileIsRegisteredToDeleteOnExitHook();
    }

    @Test
    void testFileUploadUsingCustomFilename() throws IOException {
        OkHttpClient client = createOkHttpClientWithNoTimeouts();

        String customFilename1 = "different-name-1.jar";
        String customFilename2 = "different-name-2.jar";

        multipartUpdateExtensionWrapper
                .getCustomExtension()
                .setFileUploadVerifier(
                        new CustomFilenameVerifier(
                                customFilename1,
                                multipartUpdateExtensionWrapper.getCustomExtension().file1.toPath(),
                                customFilename2,
                                multipartUpdateExtensionWrapper
                                        .getCustomExtension()
                                        .file2
                                        .toPath()));

        MessageHeaders<?, ?, ?> messageHeaders =
                multipartUpdateExtensionWrapper
                        .getCustomExtension()
                        .getFileHandler()
                        .getMessageHeaders();
        Request request =
                buildRequestWithCustomFilenames(
                        messageHeaders.getTargetRestEndpointURL(),
                        customFilename1,
                        customFilename2);
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(messageHeaders.getResponseStatusCode().code());
        }

        verifyNoFileIsRegisteredToDeleteOnExitHook();
    }

    @Test
    void testFileUploadUsingCustomFilenameWithParentFolderPath() throws IOException {
        OkHttpClient client = createOkHttpClientWithNoTimeouts();

        String customFilename1 = "different-name-1.jar";
        String customFilename2 = "different-name-2.jar";

        multipartUpdateExtensionWrapper
                .getCustomExtension()
                .setFileUploadVerifier(
                        new CustomFilenameVerifier(
                                customFilename1,
                                multipartUpdateExtensionWrapper.getCustomExtension().file1.toPath(),
                                customFilename2,
                                multipartUpdateExtensionWrapper
                                        .getCustomExtension()
                                        .file2
                                        .toPath()));

        // referring to the parent folder within the filename should be ignored
        MessageHeaders<?, ?, ?> messageHeaders =
                multipartUpdateExtensionWrapper
                        .getCustomExtension()
                        .getFileHandler()
                        .getMessageHeaders();
        Request request =
                buildRequestWithCustomFilenames(
                        multipartUpdateExtensionWrapper
                                .getCustomExtension()
                                .getFileHandler()
                                .getMessageHeaders()
                                .getTargetRestEndpointURL(),
                        String.format("../%s", customFilename1),
                        String.format("../%s", customFilename2));
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(messageHeaders.getResponseStatusCode().code());
        }

        verifyNoFileIsRegisteredToDeleteOnExitHook();
    }

    private static class CustomFilenameVerifier
            implements BiConsumerWithException<
                    HandlerRequest<? extends RequestBody>, RestfulGateway, Exception> {

        private final String customFilename1;
        private final Path fileContent1;

        private final String customFilename2;
        private final Path fileContent2;

        public CustomFilenameVerifier(
                String customFilename1,
                Path fileContent1,
                String customFilename2,
                Path fileContent2) {
            this.customFilename1 = customFilename1;
            this.fileContent1 = fileContent1;

            this.customFilename2 = customFilename2;
            this.fileContent2 = fileContent2;
        }

        @Override
        public void accept(
                HandlerRequest<? extends RequestBody> request, RestfulGateway restfulGateway)
                throws Exception {
            List<Path> uploadedFiles =
                    request.getUploadedFiles().stream()
                            .map(File::toPath)
                            .collect(Collectors.toList());

            List<Path> actualList = new ArrayList<>(uploadedFiles);
            actualList.sort(Comparator.comparing(Path::toString));

            SortedMap<String, Path> expectedFilenamesAndContent = new TreeMap<>();
            expectedFilenamesAndContent.put(customFilename1, fileContent1);
            expectedFilenamesAndContent.put(customFilename2, fileContent2);

            assertThat(expectedFilenamesAndContent).hasSameSizeAs(uploadedFiles);

            Iterator<Path> uploadedFileIterator = actualList.iterator();
            for (Map.Entry<String, Path> expectedFilenameAndContent :
                    expectedFilenamesAndContent.entrySet()) {
                String expectedFilename = expectedFilenameAndContent.getKey();
                Path expectedContent = expectedFilenameAndContent.getValue();

                assertThat(uploadedFileIterator).hasNext();
                Path actual = uploadedFileIterator.next();

                assertThat(actual.getFileName()).hasToString(expectedFilename);

                byte[] originalContent = java.nio.file.Files.readAllBytes(expectedContent);
                byte[] receivedContent = java.nio.file.Files.readAllBytes(actual);
                assertThat(receivedContent).isEqualTo(originalContent);
            }
        }
    }

    private OkHttpClient createOkHttpClientWithNoTimeouts() {
        // don't fail if some OkHttpClient operations take longer. See FLINK-17725
        return new OkHttpClient.Builder()
                .connectTimeout(0, TimeUnit.MILLISECONDS)
                .writeTimeout(0, TimeUnit.MILLISECONDS)
                .readTimeout(0, TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * DiskAttribute and DiskFileUpload class of netty store post chunks and file chunks as temp
     * files on local disk. By default, netty will register these temp files to
     * java.io.DeleteOnExitHook which may lead to memory leak. {@link FileUploadHandler} disables
     * the shutdown hook registration so no file should be registered. Note that clean up of temp
     * files is handed over to {@link org.apache.flink.runtime.entrypoint.ClusterEntrypoint}.
     */
    private void verifyNoFileIsRegisteredToDeleteOnExitHook() {
        assertThatCode(
                        () -> {
                            Class<?> clazz = Class.forName("java.io.DeleteOnExitHook");
                            Field field = clazz.getDeclaredField("files");
                            field.setAccessible(true);
                            LinkedHashSet<String> files = (LinkedHashSet<String>) field.get(null);
                            boolean fileFound = false;
                            // Mockito automatically registers mockitoboot*.jar for on-exit removal.
                            // Verify that
                            // there are no other files registered.
                            for (String file : files) {
                                if (!file.contains("mockitoboot")) {
                                    fileFound = true;
                                    break;
                                }
                            }
                            assertThat(fileFound).isFalse();
                        })
                .withFailMessage("This should never happen.")
                .doesNotThrowAnyException();
    }
}
