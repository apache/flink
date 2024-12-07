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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.cluster.FileMessageParameters;
import org.apache.flink.runtime.rest.messages.cluster.JobManagerCustomLogHeaders;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.testutils.TestingUtils;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link JobManagerCustomLogHandler}. */
class JobManagerCustomLogHandlerTest {

    private static final String FORBIDDEN_FILENAME = "forbidden";

    private static final String VALID_LOG_FILENAME = "valid.log";
    private static final String VALID_LOG_CONTENT = "logged content";

    @TempDir private java.nio.file.Path temporaryFolder;

    private File logRoot;

    private JobManagerCustomLogHandler testInstance;

    @BeforeEach
    void setUp() throws IOException {
        initializeFolderStructure();

        final TestingDispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder().build();
        testInstance =
                new JobManagerCustomLogHandler(
                        () -> CompletableFuture.completedFuture(dispatcherGateway),
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        JobManagerCustomLogHeaders.getInstance(),
                        logRoot);
    }

    private void initializeFolderStructure() throws IOException {
        File root = temporaryFolder.toFile();
        logRoot = new File(root, "logs");
        assertThat(logRoot.mkdir()).isTrue();

        createFile(new File(root, FORBIDDEN_FILENAME), "forbidden content");
        createFile(new File(logRoot, VALID_LOG_FILENAME), VALID_LOG_CONTENT);
    }

    private static void createFile(File file, String content) throws IOException {
        FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8);
    }

    private static HandlerRequest<EmptyRequestBody> createHandlerRequest(String path)
            throws HandlerRequestException {
        FileMessageParameters messageParameters = new FileMessageParameters();
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(messageParameters.logFileNamePathParameter.getKey(), path);

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                messageParameters,
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }

    @Test
    void testGetJobManagerCustomLogsValidFilename() throws Exception {
        File actualFile = testInstance.getFile(createHandlerRequest(VALID_LOG_FILENAME));
        assertThat(actualFile).isNotNull();

        String actualContent = String.join("", Files.readAllLines(actualFile.toPath()));
        assertThat(actualContent).isEqualTo(VALID_LOG_CONTENT);
    }

    @Test
    void testGetJobManagerCustomLogsValidFilenameWithPath() throws Exception {
        File actualFile =
                testInstance.getFile(
                        createHandlerRequest(String.format("foobar/%s", VALID_LOG_FILENAME)));
        assertThat(actualFile).isNotNull();

        String actualContent = String.join("", Files.readAllLines(actualFile.toPath()));
        assertThat(actualContent).isEqualTo(VALID_LOG_CONTENT);
    }

    @Test
    void testGetJobManagerCustomLogsValidFilenameWithInvalidPath() throws Exception {
        File actualFile =
                testInstance.getFile(
                        createHandlerRequest(String.format("../%s", VALID_LOG_FILENAME)));
        assertThat(actualFile).isNotNull();

        String actualContent = String.join("", Files.readAllLines(actualFile.toPath()));
        assertThat(actualContent).isEqualTo(VALID_LOG_CONTENT);
    }

    @Test
    void testGetJobManagerCustomLogsNotExistingFile() throws Exception {
        File actualFile = testInstance.getFile(createHandlerRequest("not-existing"));
        assertThat(actualFile).isNotNull().doesNotExist();
    }

    @Test
    void testGetJobManagerCustomLogsExistingButForbiddenFile() throws Exception {
        File actualFile =
                testInstance.getFile(
                        createHandlerRequest(String.format("../%s", FORBIDDEN_FILENAME)));
        assertThat(actualFile).isNotNull().doesNotExist();
    }

    @Test
    void testGetJobManagerCustomLogsValidFilenameWithLongInvalidPath() throws Exception {
        File actualFile =
                testInstance.getFile(
                        createHandlerRequest(String.format("foobar/../../%s", VALID_LOG_FILENAME)));
        assertThat(actualFile).isNotNull();

        String actualContent = String.join("", Files.readAllLines(actualFile.toPath()));
        assertThat(actualContent).isEqualTo(VALID_LOG_CONTENT);
    }
}
