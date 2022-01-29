/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.deployment.application.ApplicationRunner;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarRunMessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.JarRunRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHandler;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class WebSubmissionExtensionTest {

    private static final String JAR_NAME = "output-test-program.jar";

    @Test
    void applicationsRunInSeparateThreads(@TempDir Path tempDir) throws Exception {
        final Path uploadDir = Files.createDirectories(tempDir.resolve("uploadDir"));
        // create a copy because the upload handler moves uploaded jars (because it assumes it to be
        // a temporary file)
        final Path jarFile =
                Files.copy(
                        Paths.get(System.getProperty("targetDir")).resolve(JAR_NAME),
                        tempDir.resolve("app.jar"));

        final DispatcherGateway dispatcherGateway = TestingDispatcherGateway.newBuilder().build();

        final ThreadCapturingApplicationRunner threadCapturingApplicationRunner =
                new ThreadCapturingApplicationRunner();

        final WebSubmissionExtension webSubmissionExtension =
                new WebSubmissionExtension(
                        new Configuration(),
                        () -> CompletableFuture.completedFuture(dispatcherGateway),
                        Collections.emptyMap(),
                        new CompletableFuture<>(),
                        uploadDir,
                        Executors.directExecutor(),
                        Time.of(5, TimeUnit.SECONDS),
                        () -> threadCapturingApplicationRunner);

        final String jarId = uploadJar(webSubmissionExtension, jarFile, dispatcherGateway);

        final JarRunHandler jarRunHandler = webSubmissionExtension.getJarRunHandler();

        final JarRunMessageParameters parameters = new JarRunMessageParameters();
        parameters.jarIdPathParameter.resolve(jarId);
        final HandlerRequest<JarRunRequestBody> runRequest =
                HandlerRequest.create(new JarRunRequestBody(), parameters);

        // run several applications in sequence, and verify that each thread is unique
        int numApplications = 20;
        for (int i = 0; i < numApplications; i++) {
            jarRunHandler.handleRequest(runRequest, dispatcherGateway).get();
        }
        assertThat(threadCapturingApplicationRunner.getThreads().size()).isEqualTo(numApplications);
    }

    private static String uploadJar(
            WebSubmissionExtension extension, Path jarFile, DispatcherGateway dispatcherGateway)
            throws Exception {
        final JarUploadHandler jarUploadHandler = extension.getJarUploadHandler();

        final HandlerRequest<EmptyRequestBody> uploadRequest =
                HandlerRequest.create(
                        EmptyRequestBody.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        Collections.singletonList(jarFile.toFile()));

        return jarUploadHandler.handleRequest(uploadRequest, dispatcherGateway).get().getFilename();
    }

    private static class ThreadCapturingApplicationRunner implements ApplicationRunner {

        private final Set<Thread> threads = Collections.newSetFromMap(new IdentityHashMap<>());

        @Override
        public List<JobID> run(
                DispatcherGateway dispatcherGateway,
                PackagedProgram program,
                Configuration configuration) {
            threads.add(Thread.currentThread());
            return Collections.singletonList(new JobID());
        }

        public Collection<Thread> getThreads() {
            return threads;
        }
    }
}
