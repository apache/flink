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

import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.LogListInfo;
import org.apache.flink.runtime.rest.messages.cluster.JobManagerLogListHeaders;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Unit tests for {@link JobManagerLogListHandler}. */
public class JobManagerLogListHandlerTest extends TestLogger {

    private static HandlerRequest<EmptyRequestBody, EmptyMessageParameters> testRequest;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private DispatcherGateway dispatcherGateway;

    @BeforeClass
    public static void setupClass() throws HandlerRequestException {
        testRequest =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
    }

    @Before
    public void setUp() {
        dispatcherGateway = new TestingDispatcherGateway.Builder().build();
    }

    @Test
    public void testGetJobManagerLogsList() throws Exception {
        File logRoot = temporaryFolder.getRoot();
        List<LogInfo> expectedLogInfo =
                Arrays.asList(
                        new LogInfo("jobmanager.log", 5),
                        new LogInfo("jobmanager.out", 7),
                        new LogInfo("test.log", 13));
        createLogFiles(logRoot, expectedLogInfo);

        JobManagerLogListHandler jobManagerLogListHandler = createHandler(logRoot);
        LogListInfo logListInfo =
                jobManagerLogListHandler.handleRequest(testRequest, dispatcherGateway).get();

        assertThat(
                logListInfo.getLogInfos(),
                containsInAnyOrder(expectedLogInfo.toArray(new LogInfo[0])));
    }

    @Test
    public void testGetJobManagerLogsListWhenLogDirIsNull() throws Exception {
        JobManagerLogListHandler jobManagerLogListHandler = createHandler(null);
        LogListInfo logListInfo =
                jobManagerLogListHandler.handleRequest(testRequest, dispatcherGateway).get();

        assertThat(logListInfo.getLogInfos(), is(empty()));
    }

    private JobManagerLogListHandler createHandler(@Nullable final File jobManagerLogRoot) {
        return new JobManagerLogListHandler(
                () -> CompletableFuture.completedFuture(dispatcherGateway),
                TestingUtils.TIMEOUT,
                Collections.emptyMap(),
                JobManagerLogListHeaders.getInstance(),
                jobManagerLogRoot);
    }

    private void createLogFiles(final File logRoot, final List<LogInfo> expectedLogFiles) {
        for (LogInfo logInfo : expectedLogFiles) {
            createFile(new File(logRoot, logInfo.getName()), logInfo.getSize());
        }
    }

    private void createFile(final File file, final long size) {
        try {
            final String randomFileContent =
                    StringUtils.generateRandomAlphanumericString(
                            ThreadLocalRandom.current(), Math.toIntExact(size));
            FileUtils.writeStringToFile(file, randomFileContent, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
