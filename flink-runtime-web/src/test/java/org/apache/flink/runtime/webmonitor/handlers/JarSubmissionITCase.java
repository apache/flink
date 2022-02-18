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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.util.BlobServerExtension;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.webmonitor.handlers.JarHandlers.deleteJar;
import static org.apache.flink.runtime.webmonitor.handlers.JarHandlers.listJars;
import static org.apache.flink.runtime.webmonitor.handlers.JarHandlers.runJar;
import static org.apache.flink.runtime.webmonitor.handlers.JarHandlers.showPlan;
import static org.apache.flink.runtime.webmonitor.handlers.JarHandlers.uploadJar;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the entire lifecycle of a jar submission. */
class JarSubmissionITCase {

    @RegisterExtension
    private final EachCallbackWrapper<BlobServerExtension> blobServerExtension =
            new EachCallbackWrapper<>(new BlobServerExtension());

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testJarSubmission(@TempDir File uploadDir, @TempDir File temporaryFolder)
            throws Exception {
        final TestingDispatcherGateway restfulGateway =
                TestingDispatcherGateway.newBuilder()
                        .setBlobServerPort(
                                blobServerExtension.getCustomExtension().getBlobServerPort())
                        .setSubmitFunction(
                                jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
                        .build();

        final JarHandlers handlers =
                new JarHandlers(
                        uploadDir.toPath(), restfulGateway, EXECUTOR_EXTENSION.getExecutor());
        final JarUploadHandler uploadHandler = handlers.uploadHandler;
        final JarListHandler listHandler = handlers.listHandler;
        final JarPlanHandler planHandler = handlers.planHandler;
        final JarRunHandler runHandler = handlers.runHandler;
        final JarDeleteHandler deleteHandler = handlers.deleteHandler;

        // targetDir property is set via surefire configuration
        final Path originalJar =
                Paths.get(System.getProperty("targetDir")).resolve("test-program.jar");
        final Path jar =
                Files.copy(originalJar, temporaryFolder.toPath().resolve("test-program.jar"));

        final String storedJarPath = uploadJar(uploadHandler, jar, restfulGateway);
        final String storedJarName = Paths.get(storedJarPath).getFileName().toString();

        final JarListInfo postUploadListResponse = listJars(listHandler, restfulGateway);
        assertThat(postUploadListResponse.jarFileList).hasSize(1);
        final JarListInfo.JarFileInfo listEntry =
                postUploadListResponse.jarFileList.iterator().next();
        assertThat(listEntry.name).isEqualTo(jar.getFileName().toString());
        assertThat(listEntry.id).isEqualTo(storedJarName);

        final JobPlanInfo planResponse = showPlan(planHandler, storedJarName, restfulGateway);
        // we're only interested in the core functionality so checking for a small detail is
        // sufficient
        assertThat(planResponse.getJsonPlan()).contains("TestProgram.java:28");

        runJar(runHandler, storedJarName, restfulGateway);

        deleteJar(deleteHandler, storedJarName, restfulGateway);

        final JarListInfo postDeleteListResponse = listJars(listHandler, restfulGateway);
        assertThat(postDeleteListResponse.jarFileList).isEmpty();
    }
}
