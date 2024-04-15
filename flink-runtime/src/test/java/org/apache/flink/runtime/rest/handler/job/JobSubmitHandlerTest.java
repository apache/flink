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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.net.SSLUtilsTest;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link JobSubmitHandler}. */
@ExtendWith(ParameterizedTestExtension.class)
public class JobSubmitHandlerTest {

    @Parameters(name = "SSL enabled: {0}")
    public static Iterable<Tuple2<Boolean, String>> data() {
        ArrayList<Tuple2<Boolean, String>> parameters = new ArrayList<>(3);
        parameters.add(Tuple2.of(false, "no SSL"));
        for (String sslProvider : SSLUtilsTest.AVAILABLE_SSL_PROVIDERS) {
            parameters.add(Tuple2.of(true, sslProvider));
        }
        return parameters;
    }

    @TempDir private java.nio.file.Path temporaryFolder;

    private final Configuration configuration;

    private BlobServer blobServer;

    public JobSubmitHandlerTest(Tuple2<Boolean, String> withSsl) {
        this.configuration =
                withSsl.f0
                        ? SSLUtilsTest.createInternalSslConfigWithKeyAndTrustStores(withSsl.f1)
                        : new Configuration();
    }

    @BeforeEach
    void setup() throws IOException {
        Configuration config = new Configuration(configuration);

        blobServer =
                new BlobServer(
                        config, TempDirUtils.newFolder(temporaryFolder), new VoidBlobStore());
        blobServer.start();
    }

    @AfterEach
    void teardown() throws IOException {
        if (blobServer != null) {
            blobServer.close();
        }
    }

    @TestTemplate
    void testSerializationFailureHandling() throws Exception {
        final Path jobGraphFile = TempDirUtils.newFile(temporaryFolder).toPath();
        DispatcherGateway mockGateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
                        .build();

        JobSubmitHandler handler =
                new JobSubmitHandler(
                        () -> CompletableFuture.completedFuture(mockGateway),
                        RpcUtils.INF_TIMEOUT,
                        Collections.emptyMap(),
                        Executors.directExecutor(),
                        configuration);

        JobSubmitRequestBody request =
                new JobSubmitRequestBody(
                        jobGraphFile.toString(), Collections.emptyList(), Collections.emptyList());

        assertThatThrownBy(
                        () ->
                                handler.handleRequest(
                                        HandlerRequest.create(
                                                request, EmptyMessageParameters.getInstance()),
                                        mockGateway))
                .isInstanceOf(RestHandlerException.class)
                .satisfies(
                        e ->
                                assertThat(((RestHandlerException) e).getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.BAD_REQUEST));
    }

    @TestTemplate
    void testSuccessfulJobSubmission() throws Exception {
        final Path jobGraphFile = TempDirUtils.newFile(temporaryFolder).toPath();
        try (ObjectOutputStream objectOut =
                new ObjectOutputStream(Files.newOutputStream(jobGraphFile))) {
            objectOut.writeObject(JobGraphTestUtils.emptyJobGraph());
        }

        TestingDispatcherGateway.Builder builder = TestingDispatcherGateway.newBuilder();
        builder.setBlobServerPort(blobServer.getPort())
                .setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
                .setHostname("localhost");
        DispatcherGateway mockGateway = builder.build();

        JobSubmitHandler handler =
                new JobSubmitHandler(
                        () -> CompletableFuture.completedFuture(mockGateway),
                        RpcUtils.INF_TIMEOUT,
                        Collections.emptyMap(),
                        Executors.directExecutor(),
                        configuration);

        JobSubmitRequestBody request =
                new JobSubmitRequestBody(
                        jobGraphFile.getFileName().toString(),
                        Collections.emptyList(),
                        Collections.emptyList());

        handler.handleRequest(
                        HandlerRequest.create(
                                request,
                                EmptyMessageParameters.getInstance(),
                                Collections.singleton(jobGraphFile.toFile())),
                        mockGateway)
                .get();
    }

    @TestTemplate
    void testRejectionOnCountMismatch() throws Exception {
        final Path jobGraphFile = TempDirUtils.newFile(temporaryFolder).toPath();
        try (ObjectOutputStream objectOut =
                new ObjectOutputStream(Files.newOutputStream(jobGraphFile))) {
            objectOut.writeObject(JobGraphTestUtils.emptyJobGraph());
        }
        final Path countExceedingFile = TempDirUtils.newFile(temporaryFolder).toPath();

        TestingDispatcherGateway.Builder builder = TestingDispatcherGateway.newBuilder();
        builder.setBlobServerPort(blobServer.getPort())
                .setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
                .setHostname("localhost");
        DispatcherGateway mockGateway = builder.build();

        JobSubmitHandler handler =
                new JobSubmitHandler(
                        () -> CompletableFuture.completedFuture(mockGateway),
                        RpcUtils.INF_TIMEOUT,
                        Collections.emptyMap(),
                        Executors.directExecutor(),
                        configuration);

        JobSubmitRequestBody request =
                new JobSubmitRequestBody(
                        jobGraphFile.getFileName().toString(),
                        Collections.emptyList(),
                        Collections.emptyList());
        try {
            handler.handleRequest(
                            HandlerRequest.create(
                                    request,
                                    EmptyMessageParameters.getInstance(),
                                    Arrays.asList(
                                            jobGraphFile.toFile(), countExceedingFile.toFile())),
                            mockGateway)
                    .get();
        } catch (Exception e) {
            ExceptionUtils.findThrowable(
                    e,
                    candidate ->
                            candidate instanceof RestHandlerException
                                    && candidate.getMessage().contains("count"));
        }
    }

    @TestTemplate
    void testFileHandling() throws Exception {
        final String dcEntryName = "entry";

        CompletableFuture<JobGraph> submittedJobGraphFuture = new CompletableFuture<>();
        DispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder()
                        .setBlobServerPort(blobServer.getPort())
                        .setSubmitFunction(
                                submittedJobGraph -> {
                                    submittedJobGraphFuture.complete(submittedJobGraph);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        JobSubmitHandler handler =
                new JobSubmitHandler(
                        () -> CompletableFuture.completedFuture(dispatcherGateway),
                        RpcUtils.INF_TIMEOUT,
                        Collections.emptyMap(),
                        Executors.directExecutor(),
                        configuration);

        final Path jobGraphFile = TempDirUtils.newFile(temporaryFolder).toPath();
        final Path jarFile = TempDirUtils.newFile(temporaryFolder).toPath();
        final Path artifactFile = TempDirUtils.newFile(temporaryFolder).toPath();

        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        // the entry that should be updated
        jobGraph.addUserArtifact(
                dcEntryName, new DistributedCache.DistributedCacheEntry("random", false));
        try (ObjectOutputStream objectOut =
                new ObjectOutputStream(Files.newOutputStream(jobGraphFile))) {
            objectOut.writeObject(jobGraph);
        }

        JobSubmitRequestBody request =
                new JobSubmitRequestBody(
                        jobGraphFile.getFileName().toString(),
                        Collections.singletonList(jarFile.getFileName().toString()),
                        Collections.singleton(
                                new JobSubmitRequestBody.DistributedCacheFile(
                                        dcEntryName, artifactFile.getFileName().toString())));

        handler.handleRequest(
                        HandlerRequest.create(
                                request,
                                EmptyMessageParameters.getInstance(),
                                Arrays.asList(
                                        jobGraphFile.toFile(),
                                        jarFile.toFile(),
                                        artifactFile.toFile())),
                        dispatcherGateway)
                .get();

        assertThat(submittedJobGraphFuture).as("No JobGraph was submitted.").isCompleted();
        final JobGraph submittedJobGraph = submittedJobGraphFuture.get();
        assertThat(submittedJobGraph.getUserJarBlobKeys()).hasSize(1);
        assertThat(submittedJobGraph.getUserArtifacts()).hasSize(1);
        assertThat(submittedJobGraph.getUserArtifacts().get(dcEntryName).blobKey).isNotNull();
    }

    @TestTemplate
    void testFailedJobSubmission() throws Exception {
        final String errorMessage = "test";
        DispatcherGateway mockGateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobgraph ->
                                        FutureUtils.completedExceptionally(
                                                new Exception(errorMessage)))
                        .build();

        JobSubmitHandler handler =
                new JobSubmitHandler(
                        () -> CompletableFuture.completedFuture(mockGateway),
                        RpcUtils.INF_TIMEOUT,
                        Collections.emptyMap(),
                        Executors.directExecutor(),
                        configuration);

        final Path jobGraphFile = TempDirUtils.newFile(temporaryFolder).toPath();

        JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        try (ObjectOutputStream objectOut =
                new ObjectOutputStream(Files.newOutputStream(jobGraphFile))) {
            objectOut.writeObject(jobGraph);
        }
        JobSubmitRequestBody request =
                new JobSubmitRequestBody(
                        jobGraphFile.getFileName().toString(),
                        Collections.emptyList(),
                        Collections.emptyList());

        assertThatFuture(
                        handler.handleRequest(
                                HandlerRequest.create(
                                        request,
                                        EmptyMessageParameters.getInstance(),
                                        Collections.singletonList(jobGraphFile.toFile())),
                                mockGateway))
                .eventuallyFailsWith(Exception.class)
                .withMessageContaining(errorMessage);
    }
}
