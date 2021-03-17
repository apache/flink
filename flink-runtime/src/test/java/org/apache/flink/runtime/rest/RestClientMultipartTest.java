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
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Tests for the multipart functionality of the {@link RestClient}. */
public class RestClientMultipartTest extends TestLogger {

    @ClassRule
    public static final MultipartUploadResource MULTIPART_UPLOAD_RESOURCE =
            new MultipartUploadResource();

    private static RestClient restClient;

    @BeforeClass
    public static void setupClient() throws ConfigurationException {
        restClient =
                new RestClient(
                        RestClientConfiguration.fromConfiguration(new Configuration()),
                        TestingUtils.defaultExecutor());
    }

    @After
    public void reset() {
        MULTIPART_UPLOAD_RESOURCE.resetState();
    }

    @AfterClass
    public static void teardownClient() {
        if (restClient != null) {
            restClient.shutdown(Time.seconds(10));
        }
    }

    @Test
    public void testMixedMultipart() throws Exception {
        Collection<FileUpload> files =
                MULTIPART_UPLOAD_RESOURCE.getFilesToUpload().stream()
                        .map(file -> new FileUpload(file.toPath(), "application/octet-stream"))
                        .collect(Collectors.toList());

        MultipartUploadResource.TestRequestBody json =
                new MultipartUploadResource.TestRequestBody();
        CompletableFuture<EmptyResponseBody> responseFuture =
                restClient.sendRequest(
                        MULTIPART_UPLOAD_RESOURCE.getServerSocketAddress().getHostName(),
                        MULTIPART_UPLOAD_RESOURCE.getServerSocketAddress().getPort(),
                        MULTIPART_UPLOAD_RESOURCE.getMixedHandler().getMessageHeaders(),
                        EmptyMessageParameters.getInstance(),
                        json,
                        files);

        responseFuture.get();
        Assert.assertEquals(json, MULTIPART_UPLOAD_RESOURCE.getMixedHandler().lastReceivedRequest);
    }

    @Test
    public void testJsonMultipart() throws Exception {
        MultipartUploadResource.TestRequestBody json =
                new MultipartUploadResource.TestRequestBody();
        CompletableFuture<EmptyResponseBody> responseFuture =
                restClient.sendRequest(
                        MULTIPART_UPLOAD_RESOURCE.getServerSocketAddress().getHostName(),
                        MULTIPART_UPLOAD_RESOURCE.getServerSocketAddress().getPort(),
                        MULTIPART_UPLOAD_RESOURCE.getJsonHandler().getMessageHeaders(),
                        EmptyMessageParameters.getInstance(),
                        json,
                        Collections.emptyList());

        responseFuture.get();
        Assert.assertEquals(json, MULTIPART_UPLOAD_RESOURCE.getJsonHandler().lastReceivedRequest);
    }

    @Test
    public void testFileMultipart() throws Exception {
        Collection<FileUpload> files =
                MULTIPART_UPLOAD_RESOURCE.getFilesToUpload().stream()
                        .map(file -> new FileUpload(file.toPath(), "application/octet-stream"))
                        .collect(Collectors.toList());

        CompletableFuture<EmptyResponseBody> responseFuture =
                restClient.sendRequest(
                        MULTIPART_UPLOAD_RESOURCE.getServerSocketAddress().getHostName(),
                        MULTIPART_UPLOAD_RESOURCE.getServerSocketAddress().getPort(),
                        MULTIPART_UPLOAD_RESOURCE.getFileHandler().getMessageHeaders(),
                        EmptyMessageParameters.getInstance(),
                        EmptyRequestBody.getInstance(),
                        files);

        responseFuture.get();
    }
}
