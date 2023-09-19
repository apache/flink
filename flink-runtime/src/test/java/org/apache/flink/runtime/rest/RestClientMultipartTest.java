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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the multipart functionality of the {@link RestClient}. */
class RestClientMultipartTest {

    @TempDir private static Path tempDir;

    @RegisterExtension
    private static final AllCallbackWrapper<MultipartUploadExtension>
            MULTIPART_UPLOAD_EXTENSION_WRAPPER =
                    new AllCallbackWrapper<>(new MultipartUploadExtension(() -> tempDir));

    private static RestClient restClient;

    public static MultipartUploadExtension multipartUploadExtension;

    @BeforeAll
    static void setupClient() throws ConfigurationException {
        multipartUploadExtension = MULTIPART_UPLOAD_EXTENSION_WRAPPER.getCustomExtension();
        restClient = new RestClient(new Configuration(), Executors.directExecutor());
    }

    @AfterEach
    void reset() {
        MULTIPART_UPLOAD_EXTENSION_WRAPPER.getCustomExtension().resetState();
    }

    @AfterAll
    static void teardownClient() {
        if (restClient != null) {
            restClient.shutdown(Time.seconds(10));
        }
    }

    @Test
    void testMixedMultipart() throws Exception {
        Collection<FileUpload> files =
                multipartUploadExtension.getFilesToUpload().stream()
                        .map(file -> new FileUpload(file.toPath(), "application/octet-stream"))
                        .collect(Collectors.toList());

        MultipartUploadExtension.TestRequestBody json =
                new MultipartUploadExtension.TestRequestBody();
        CompletableFuture<EmptyResponseBody> responseFuture =
                restClient.sendRequest(
                        multipartUploadExtension.getServerSocketAddress().getHostName(),
                        multipartUploadExtension.getServerSocketAddress().getPort(),
                        multipartUploadExtension.getMixedHandler().getMessageHeaders(),
                        EmptyMessageParameters.getInstance(),
                        json,
                        files);

        responseFuture.get();
        assertThat(json).isEqualTo(multipartUploadExtension.getMixedHandler().lastReceivedRequest);
    }

    @Test
    void testJsonMultipart() throws Exception {
        MultipartUploadExtension.TestRequestBody json =
                new MultipartUploadExtension.TestRequestBody();
        CompletableFuture<EmptyResponseBody> responseFuture =
                restClient.sendRequest(
                        multipartUploadExtension.getServerSocketAddress().getHostName(),
                        multipartUploadExtension.getServerSocketAddress().getPort(),
                        multipartUploadExtension.getJsonHandler().getMessageHeaders(),
                        EmptyMessageParameters.getInstance(),
                        json,
                        Collections.emptyList());

        responseFuture.get();
        assertThat(json).isEqualTo(multipartUploadExtension.getJsonHandler().lastReceivedRequest);
    }

    @Test
    void testFileMultipart() throws Exception {
        Collection<FileUpload> files =
                multipartUploadExtension.getFilesToUpload().stream()
                        .map(file -> new FileUpload(file.toPath(), "application/octet-stream"))
                        .collect(Collectors.toList());

        CompletableFuture<EmptyResponseBody> responseFuture =
                restClient.sendRequest(
                        multipartUploadExtension.getServerSocketAddress().getHostName(),
                        multipartUploadExtension.getServerSocketAddress().getPort(),
                        multipartUploadExtension.getFileHandler().getMessageHeaders(),
                        EmptyMessageParameters.getInstance(),
                        EmptyRequestBody.getInstance(),
                        files);

        responseFuture.get();
    }
}
