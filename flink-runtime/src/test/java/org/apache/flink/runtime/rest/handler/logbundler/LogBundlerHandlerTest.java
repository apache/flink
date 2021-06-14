/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.logbundler;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.taskmanager.AbstractTaskManagerFileHandlerTest;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.logbundler.LogBundlerHeaders;
import org.apache.flink.runtime.rest.messages.logbundler.LogBundlerMessageParameters;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test for the {@link LogBundlerHandler}. The handler is also tested in {@see
 * WebFrontendITCase#testLogBundler()}.
 */
public class LogBundlerHandlerTest extends TestLogger {

    private static final Time TEST_TIMEOUT = Time.seconds(10);
    private static HandlerRequest<EmptyRequestBody, LogBundlerMessageParameters> handlerRequest;
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final DefaultFullHttpRequest HTTP_REQUEST =
            new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1,
                    HttpMethod.GET,
                    AbstractTaskManagerFileHandlerTest.TestUntypedMessageHeaders.URL);
    private static BlobServer blobServer;

    @BeforeClass
    public static void prepare() throws IOException, HandlerRequestException {
        handlerRequest =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(), new LogBundlerMessageParameters());

        Configuration blobServerConfig = new Configuration();
        blobServerConfig.setString(
                BlobServerOptions.STORAGE_DIRECTORY,
                TEMPORARY_FOLDER.newFolder().getAbsolutePath());

        blobServer = new BlobServer(blobServerConfig, new VoidBlobStore());
    }

    @AfterClass
    public static void cleanup() throws IOException {
        blobServer.close();
    }

    @Test
    public void testIdleByDefault() throws Exception {
        final TestContext testingContext = new TestContext();

        createHandler().respondToRequest(testingContext, HTTP_REQUEST, handlerRequest, null);

        assertThat(testingContext.getResponse(), containsString("IDLE"));
    }

    @Test(expected = RestHandlerException.class)
    public void testNoTmpDir() throws Exception {
        Configuration config = new Configuration();
        config.set(CoreOptions.TMP_DIRS, "");
        LogBundlerHandler handler = createHandler(config);

        // assert that status queries are possible even if no tmp dir is set
        final TestContext testingContext = new TestContext();
        handler.respondToRequest(testingContext, HTTP_REQUEST, handlerRequest, null);
        assertThat(testingContext.getResponse(), containsString("IDLE"));

        handler.respondToRequest(
                new TestContext(), HTTP_REQUEST, createActionRequest("trigger"), null);
    }

    @Test(expected = RestHandlerException.class)
    public void testDownloadWhenNotReady() throws Exception {
        LogBundlerHandler handler = createHandler();

        final TestContext testingContext = new TestContext();

        handler.respondToRequest(
                testingContext, HTTP_REQUEST, createActionRequest("download"), null);
    }

    /** Creating the log archive will fail because the RM gateway is null in this test setup. */
    @Test
    public void testErrorWhileProcessing() throws Exception {
        LogBundlerHandler handler = createHandler();

        final TestContext testingContext = new TestContext();

        handler.respondToRequest(
                testingContext, HTTP_REQUEST, createActionRequest("trigger"), null);

        // ensure that we are in state PROCESSING in the response to the trigger
        assertThat(testingContext.getResponse(), containsString("PROCESSING"));

        // wait until processing failed
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final TestContext ctx = new TestContext();

                    handler.respondToRequest(ctx, HTTP_REQUEST, handlerRequest, null);
                    return ctx.getResponse().contains("BUNDLE_FAILED");
                },
                Deadline.fromNow(Duration.ofSeconds(10)));
    }

    private HandlerRequest<EmptyRequestBody, LogBundlerMessageParameters> createActionRequest(
            String action) throws HandlerRequestException {
        Map<String, List<String>> queryParameters =
                Collections.singletonMap("action", Collections.singletonList(action));
        return new HandlerRequest<>(
                EmptyRequestBody.getInstance(),
                new LogBundlerMessageParameters(),
                Collections.emptyMap(),
                queryParameters);
    }

    private LogBundlerHandler createHandler() {
        return createHandler(new Configuration());
    }

    private LogBundlerHandler createHandler(Configuration config) {
        return new LogBundlerHandler(
                () -> CompletableFuture.completedFuture(null),
                TEST_TIMEOUT,
                Collections.emptyMap(),
                LogBundlerHeaders.getInstance(),
                executor,
                config,
                null,
                () -> CompletableFuture.completedFuture(null),
                blobServer);
    }

    private static class TestContext
            extends AbstractTaskManagerFileHandlerTest.TestingChannelHandlerContext {

        public TestContext() throws IOException {
            super(TEMPORARY_FOLDER.newFile());
        }

        public String getResponse() throws IOException {
            return FileUtils.readFileUtf8(getOutputFile());
        }
    }
}
