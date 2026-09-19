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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions.HistoryServerArchiveLoadMode;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.webmonitor.testutils.HttpUtils;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.configuration.HistoryServerOptions.HistoryServerArchiveLoadMode.EAGER;
import static org.apache.flink.configuration.HistoryServerOptions.HistoryServerArchiveLoadMode.LAZY;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.createJobArchive;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.createRefreshLocation;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Common HTTP-level tests for {@link AbstractHistoryServerHandler} subclasses. New subclasses can
 * be exercised by adding an entry to {@link #handlerFactories()}.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class AbstractHistoryServerHandlerTest {

    @Parameter public HandlerFactory handlerFactory;

    @TempDir private Path tmpDir;
    private Path uploadDir;
    private Path remoteArchiveRootPath;

    private Path webDir;
    private AbstractHistoryServerHandler<?> handler;
    private WebFrontendBootstrap webUI;
    private String baseUrl;

    /** Factory that creates a concrete handler bound to the given web directory. */
    @FunctionalInterface
    public interface HandlerFactory {
        AbstractHistoryServerHandler<?> create(
                File webDir,
                HistoryServerArchiveLoadMode archiveLoadMode,
                List<HistoryServer.RefreshLocation> refreshDirs)
                throws Exception;
    }

    /** Constructor reference for a concrete {@link AbstractHistoryServerHandler} subclass. */
    @FunctionalInterface
    private interface HandlerConstructor<T> {
        AbstractHistoryServerHandler<T> create(
                ArchiveStorage<T> archiveStorage,
                HistoryServerArchiveLoadMode archiveLoadMode,
                HistoryServerArchiveFetcher<T> archiveFetcher,
                HistoryServerApplicationArchiveFetcher<T> applicationArchiveFetcher,
                File webDir)
                throws IOException;
    }

    /**
     * Provides the concrete {@link AbstractHistoryServerHandler} implementations under test. To
     * cover an additional handler simply add another factory here.
     */
    @Parameters(name = "handlerFactory={0}")
    private static Collection<HandlerFactory> handlerFactories() {
        HandlerFactory staticFileServerHandlerFactory =
                (webDir, mode, refreshDirs) ->
                        buildHandler(
                                webDir,
                                mode,
                                refreshDirs,
                                new FileArchiveStorage(webDir),
                                HistoryServerStaticFileServerHandler::new);

        HandlerFactory rocksDBHandlerFactory =
                (webDir, mode, refreshDirs) -> {
                    File dbPath = new File(webDir, "rocksdb-" + UUID.randomUUID());
                    Files.createDirectories(dbPath.toPath());
                    return buildHandler(
                            webDir,
                            mode,
                            refreshDirs,
                            new RocksDBArchiveStorage(dbPath, new Configuration()),
                            HistoryServerRocksDBHandler::new);
                };

        return Arrays.asList(staticFileServerHandlerFactory, rocksDBHandlerFactory);
    }

    private static <T> AbstractHistoryServerHandler<?> buildHandler(
            File webDir,
            HistoryServerArchiveLoadMode mode,
            List<HistoryServer.RefreshLocation> refreshDirs,
            ArchiveStorage<T> baseStorage,
            HandlerConstructor<T> handlerCtor)
            throws Exception {
        ArchiveStorage<T> storage =
                LAZY == mode
                        ? new HistoryServerTestUtils.BlockingArchiveStorage<>(
                                baseStorage, "/config")
                        : baseStorage;

        ConcurrentHashMap<String, ArchiveMetaInfo> jobMetaInfoCache = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, ArchiveMetaInfo> applicationMetaInfoCache =
                new ConcurrentHashMap<>();

        HistoryServerArchiveFetcher<T> archiveFetcher =
                new HistoryServerArchiveFetcher<>(
                        refreshDirs,
                        webDir,
                        ignored -> {},
                        false,
                        HistoryServerTestUtils.RETAIN_ALL,
                        storage,
                        jobMetaInfoCache,
                        4,
                        4);
        HistoryServerApplicationArchiveFetcher<T> applicationArchiveFetcher =
                new HistoryServerApplicationArchiveFetcher<>(
                        refreshDirs,
                        webDir,
                        ignored -> {},
                        false,
                        HistoryServerTestUtils.RETAIN_ALL,
                        storage,
                        jobMetaInfoCache,
                        applicationMetaInfoCache,
                        4,
                        4);

        return handlerCtor.create(storage, mode, archiveFetcher, applicationArchiveFetcher, webDir);
    }

    @BeforeEach
    void setUp() throws Exception {
        webDir = Files.createDirectory(tmpDir.resolve("webDir"));
        uploadDir = Files.createDirectory(tmpDir.resolve("uploadDir"));
        remoteArchiveRootPath = Files.createDirectories(tmpDir.resolve("remote"));

        // Default: eager mode
        startServer(EAGER);
    }

    @AfterEach
    void tearDown() {
        stopServer();
    }

    /** Support recreates the handler and web frontend for the given load mode. */
    private void startServer(HistoryServerArchiveLoadMode archiveLoadMode) throws Exception {
        stopServer();

        List<HistoryServer.RefreshLocation> refreshDirs =
                Collections.singletonList(createRefreshLocation(remoteArchiveRootPath.toFile()));

        this.handler = handlerFactory.create(webDir.toFile(), archiveLoadMode, refreshDirs);

        Router<?> router = new Router().addGet("/:*", handler);
        webUI =
                new WebFrontendBootstrap(
                        router,
                        LoggerFactory.getLogger(getClass()),
                        uploadDir.toFile(),
                        null,
                        "localhost",
                        0,
                        new Configuration());
        baseUrl = "http://localhost:" + webUI.getServerPort();
    }

    private void stopServer() {
        if (webUI != null) {
            webUI.shutdown();
            webUI = null;
        }
    }

    /**
     * Tests requests against static files served from the {@code web/} resources packaged with the
     * handler module:
     *
     * <ul>
     *   <li>a request for {@code /} is rewritten to {@code /index.html};
     *   <li>{@code /index.html} is loaded via the classloader fallback when not present on disk;
     *   <li>a missing static file (e.g. {@code /hello.html}) results in 404.
     * </ul>
     */
    @TestTemplate
    void testRespondWithStaticFile() throws Exception {
        // /index.html is loaded from the classloader (web/index.html) and served
        Tuple2<Integer, String> index = HttpUtils.getFromHTTP(baseUrl + "/index.html");
        assertThat(index.f0).isEqualTo(200);
        assertThat(index.f1).contains("Apache Flink Web Dashboard");

        // a trailing slash is rewritten to "/index.html"
        Tuple2<Integer, String> index2 = HttpUtils.getFromHTTP(baseUrl + "/");
        assertThat(index2).isEqualTo(index);

        // a static file that is neither on disk nor in the classloader yields 404
        Tuple2<Integer, String> missing = HttpUtils.getFromHTTP(baseUrl + "/hello.html");
        assertThat(missing.f0).isEqualTo(404);
        assertThat(missing.f1).contains("not found");
    }

    /**
     * Tests file-system safety checks performed by {@code responseWithFile}:
     *
     * <ul>
     *   <li>a directory request returns 405;
     *   <li>a path that escapes the {@code webDir} returns 403.
     * </ul>
     */
    @TestTemplate
    void testRespondWithFile() throws Exception {
        // requesting a directory rather than a file yields 405
        Files.createDirectory(webDir.resolve("dir.json"));
        Tuple2<Integer, String> dirNotFound = HttpUtils.getFromHTTP(baseUrl + "/dir.json");
        assertThat(dirNotFound.f0).isEqualTo(405);
        assertThat(dirNotFound.f1).contains("not found");

        // requesting a file outside of the webDir is rejected with 403
        Files.createFile(tmpDir.resolve("secret"));
        Tuple2<Integer, String> outsideWebDir = HttpUtils.getFromHTTP(baseUrl + "/../secret");
        assertThat(outsideWebDir.f0).isEqualTo(403);
        assertThat(outsideWebDir.f1).contains("Forbidden");
    }

    /**
     * Tests requests served via the {@link ArchiveStorage} resource.
     *
     * <ul>
     *   <li>an existing entry is returned with its content;
     *   <li>a missing entry results in 404.
     * </ul>
     */
    @TestTemplate
    void testRespondWithResource() throws Exception {
        String resourcePath = "overviews/job1.json";
        String resourceContent = "{\"job\":\"job1\"}";
        handler.archiveStorage.putArchiveContent(resourcePath, resourceContent);

        // request without an extension: handler will append ".json" and serve the
        // entry from the ArchiveStorage via respondWithResource
        Tuple2<Integer, String> resource = HttpUtils.getFromHTTP(baseUrl + "/overviews/job1");
        assertThat(resource.f0).isEqualTo(200);
        assertThat(resource.f1).isEqualTo(resourceContent);

        // request a missing resource: archiveStorage returns null and the handler
        // responds 404
        Tuple2<Integer, String> missing = HttpUtils.getFromHTTP(baseUrl + "/hello");
        assertThat(missing.f0).isEqualTo(404);
        assertThat(missing.f1).contains("not found");
    }

    /**
     * Tests {@code AbstractHistoryServerHandler#loadResource} in {@code LAZY} mode using a {@link
     * HistoryServerTestUtils.BlockingArchiveStorage} to suspend writes for the detail key {@code
     * jobs/<jobId>/config}.
     *
     * <p>Verifies the three core lazy-load behaviours:
     *
     * <ul>
     *   <li>requesting {@code /jobs/overview} triggers a synchronous phase-1 fetch that writes the
     *       overview keys but leaves the detail key blocked in an asynchronous task;
     *   <li>once phase-1 has completed, requesting {@code /jobs/<jobId>} is served immediately from
     *       the archive storage without waiting for the asynchronous detail task;
     *   <li>requesting {@code /jobs/<jobId>/config} blocks until the asynchronous detail task is
     *       allowed to finish, after which the response is served successfully.
     * </ul>
     */
    @TestTemplate
    void testLazyModeLoadResource() throws Exception {
        // Default mode is EAGER, we need to recreate the handler for LAZY mode with
        // BlockingArchiveStorage.
        startServer(LAZY);

        JobID jobId = JobID.generate();
        createJobArchive(remoteArchiveRootPath.toFile(), jobId, true);

        // Phase 1: requesting /jobs/overview triggers a synchronous fetch
        // that writes the overview keys; the detail key is queued for an
        // asynchronous write that is now blocked on releaseLatch.
        Tuple2<Integer, String> overviewResponse =
                HttpUtils.getFromHTTP(baseUrl + "/jobs/overview");
        assertThat(overviewResponse.f0).isEqualTo(200);
        assertThat(overviewResponse.f1).contains(jobId.toString());

        // The asynchronous detail write must have been reached by now.
        assertThat(handler.archiveStorage)
                .isInstanceOf(HistoryServerTestUtils.BlockingArchiveStorage.class);
        HistoryServerTestUtils.BlockingArchiveStorage<?> blockingArchiveStorage =
                (HistoryServerTestUtils.BlockingArchiveStorage<?>) handler.archiveStorage;
        boolean asyncReached = blockingArchiveStorage.asyncStartLatch.await(10, TimeUnit.SECONDS);
        assertThat(asyncReached).isTrue();

        // Phase 1 keys are present, but the detail key is still blocked.
        assertThat(blockingArchiveStorage.exists("overviews/" + jobId + ".json")).isTrue();
        assertThat(blockingArchiveStorage.exists("jobs/" + jobId + ".json")).isTrue();
        assertThat(blockingArchiveStorage.exists("jobs/overview.json")).isTrue();
        assertThat(blockingArchiveStorage.exists("jobs/" + jobId + "/config.json")).isFalse();

        // Phase 2: a request for /jobs/<jobId> can be served immediately
        // from the storage even though the detail task is still blocked.
        Tuple2<Integer, String> jobResponse = HttpUtils.getFromHTTP(baseUrl + "/jobs/" + jobId);
        assertThat(jobResponse.f0).isEqualTo(200);
        assertThat(jobResponse.f1).contains(jobId.toString());
        assertThat(blockingArchiveStorage.exists("jobs/" + jobId + "/config.json")).isFalse();

        // Phase 3: a request for /jobs/<jobId>/config will wait for the
        // asynchronous detail task to finish. Issue it on a separate
        // thread, verify it is still in flight, then release the latch.
        AtomicReference<Tuple2<Integer, String>> detailResponse = new AtomicReference<>();
        CompletableFuture<Void> detailFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                detailResponse.set(
                                        HttpUtils.getFromHTTP(
                                                baseUrl + "/jobs/" + jobId + "/config"));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        // Give the detail request enough time to reach
        // waitLazyFetchArchiveFinished and block on the still-suspended async
        // detail task. It must not have completed yet.
        Thread.sleep(500);
        assertThat(detailFuture.isDone()).isFalse();

        blockingArchiveStorage.releaseLatch.countDown();

        detailFuture.get(10, TimeUnit.SECONDS);
        assertThat(detailResponse.get().f0).isEqualTo(200);
        assertThat(detailResponse.get().f1).contains(jobId.toString());
        assertThat(blockingArchiveStorage.exists("jobs/" + jobId + "/config.json")).isTrue();
    }
}
