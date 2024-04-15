/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.program.artifact;

import org.apache.flink.client.cli.ArtifactFetchOptions;
import org.apache.flink.configuration.Configuration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ArtifactFetchManager}. */
class ArtifactFetchManagerTest {

    private static final Logger LOG = LoggerFactory.getLogger(ArtifactFetchManagerTest.class);

    @TempDir private Path tempDir;

    private Configuration configuration;

    @BeforeEach
    void setup() {
        configuration = new Configuration();
        configuration.setString(ArtifactFetchOptions.BASE_DIR, tempDir.toAbsolutePath().toString());
    }

    @Test
    void testGetFetcher() throws Exception {
        configuration.setBoolean(ArtifactFetchOptions.RAW_HTTP_ENABLED, true);
        ArtifactFetchManager fetchManager = new ArtifactFetchManager(configuration);

        ArtifactFetcher fetcher = fetchManager.getFetcher(new URI("local:///a.jar"));
        assertThat(fetcher).isInstanceOf(LocalArtifactFetcher.class);

        fetcher = fetchManager.getFetcher(new URI("http://0.0.0.0:123/a.jar"));
        assertThat(fetcher).isInstanceOf(HttpArtifactFetcher.class);

        fetcher = fetchManager.getFetcher(new URI("https://0.0.0.0:123/a.jar"));
        assertThat(fetcher).isInstanceOf(HttpArtifactFetcher.class);

        fetcher = fetchManager.getFetcher(new URI("hdfs:///tmp/a.jar"));
        assertThat(fetcher).isInstanceOf(FsArtifactFetcher.class);

        fetcher = fetchManager.getFetcher(new URI("s3a:///tmp/a.jar"));
        assertThat(fetcher).isInstanceOf(FsArtifactFetcher.class);
    }

    @Test
    void testFileSystemFetchWithoutAdditionalUri() throws Exception {
        File sourceFile = getDummyArtifact(getClass());
        String uriStr = "file://" + sourceFile.toURI().getPath();

        ArtifactFetchManager fetchMgr = new ArtifactFetchManager(configuration);
        ArtifactFetchManager.Result res = fetchMgr.fetchArtifacts(uriStr, null);
        assertThat(res.getJobJar()).exists();
        assertThat(res.getJobJar().getName()).isEqualTo(sourceFile.getName());
        assertThat(res.getArtifacts()).isNull();
    }

    @Test
    void testFileSystemFetchWithAdditionalUri() throws Exception {
        File sourceFile = getDummyArtifact(getClass());
        String uriStr = "file://" + sourceFile.toURI().getPath();
        File additionalSrcFile = getFlinkClientsJar();
        String additionalUriStr = "file://" + additionalSrcFile.toURI().getPath();

        ArtifactFetchManager fetchMgr = new ArtifactFetchManager(configuration);
        ArtifactFetchManager.Result res =
                fetchMgr.fetchArtifacts(uriStr, Collections.singletonList(additionalUriStr));
        assertThat(res.getJobJar()).exists();
        assertThat(res.getJobJar().getName()).isEqualTo(sourceFile.getName());
        assertThat(res.getArtifacts()).hasSize(1);
        File additionalFetched = res.getArtifacts().get(0);
        assertThat(additionalFetched.getName()).isEqualTo(additionalSrcFile.getName());
    }

    @Test
    void testHttpFetch() throws Exception {
        configuration.setBoolean(ArtifactFetchOptions.RAW_HTTP_ENABLED, true);
        HttpServer httpServer = null;
        try {
            httpServer = startHttpServer();
            File sourceFile = getFlinkClientsJar();
            httpServer.createContext(
                    "/download/" + sourceFile.getName(), new DummyHttpDownloadHandler(sourceFile));
            String uriStr =
                    String.format(
                            "http://127.0.0.1:%d/download/" + sourceFile.getName(),
                            httpServer.getAddress().getPort());

            ArtifactFetchManager fetchMgr = new ArtifactFetchManager(configuration);
            ArtifactFetchManager.Result res = fetchMgr.fetchArtifacts(new String[] {uriStr});
            assertThat(res.getJobJar()).isNotNull();
            assertThat(res.getArtifacts()).isNull();
            assertFetchedFile(res.getJobJar(), sourceFile);
        } finally {
            if (httpServer != null) {
                httpServer.stop(0);
            }
        }
    }

    @Test
    void testMixedArtifactFetch() throws Exception {
        File sourceFile = getDummyArtifact(getClass());
        String uriStr = "file://" + sourceFile.toURI().getPath();
        File sourceFile2 = getFlinkClientsJar();
        String uriStr2 = "file://" + sourceFile2.toURI().getPath();

        ArtifactFetchManager fetchMgr = new ArtifactFetchManager(configuration);
        ArtifactFetchManager.Result res = fetchMgr.fetchArtifacts(new String[] {uriStr, uriStr2});
        assertThat(res.getJobJar()).isNull();
        assertThat(res.getArtifacts()).hasSize(2);
        assertFetchedFile(res.getArtifacts().get(0), sourceFile);
        assertFetchedFile(res.getArtifacts().get(1), sourceFile2);
    }

    @Test
    void testNoFetchOverride() throws Exception {
        DummyFetcher dummyFetcher = new DummyFetcher();
        ArtifactFetchManager fetchMgr =
                new ArtifactFetchManager(
                        dummyFetcher, dummyFetcher, dummyFetcher, configuration, null);

        File sourceFile = getDummyArtifact(getClass());
        Path destFile = tempDir.resolve(sourceFile.getName());
        Files.copy(sourceFile.toPath(), destFile);

        String uriStr = "file://" + sourceFile.toURI().getPath();
        fetchMgr.fetchArtifacts(uriStr, null);

        assertThat(dummyFetcher.fetchCount).isZero();
    }

    @Test
    void testHttpDisabledError() {
        ArtifactFetchManager fetchMgr = new ArtifactFetchManager(configuration);
        assertThatThrownBy(
                        () ->
                                fetchMgr.fetchArtifacts(
                                        "http://127.0.0.1:1234/download/notexists.jar", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("raw HTTP endpoints are disabled");
    }

    @Test
    void testMissingRequiredFetchArgs() {
        ArtifactFetchManager fetchMgr = new ArtifactFetchManager(configuration);
        assertThatThrownBy(() -> fetchMgr.fetchArtifacts(null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The jobUri is required.");

        assertThatThrownBy(() -> fetchMgr.fetchArtifacts(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("At least one URI is required.");

        assertThatThrownBy(() -> fetchMgr.fetchArtifacts(new String[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("At least one URI is required.");
    }

    private void assertFetchedFile(File actual, File expected) {
        assertThat(actual).exists();
        assertThat(actual.getName()).isEqualTo(expected.getName());
        assertThat(actual.length()).isEqualTo(expected.length());
    }

    private HttpServer startHttpServer() throws IOException {
        int port = RandomUtils.nextInt(2000, 3000);
        HttpServer httpServer = null;
        while (httpServer == null && port <= 65536) {
            try {
                httpServer = HttpServer.create(new InetSocketAddress(port), 0);
                httpServer.setExecutor(null);
                httpServer.start();
            } catch (BindException e) {
                LOG.warn("Failed to start http server", e);
                port++;
            }
        }
        return httpServer;
    }

    private File getDummyArtifact(Class<?> cls) {
        String className = String.format("%s.class", cls.getSimpleName());
        URL url = cls.getResource(className);
        assertThat(url).isNotNull();

        return new File(url.getPath());
    }

    private File getFlinkClientsJar() throws IOException {
        String pathStr =
                ArtifactFetchManager.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation()
                        .getPath();
        Path mvnTargetDir = Paths.get(pathStr).getParent();

        Collection<Path> jarPaths =
                org.apache.flink.util.FileUtils.listFilesInDirectory(
                        mvnTargetDir,
                        p ->
                                org.apache.flink.util.FileUtils.isJarFile(p)
                                        && p.toFile().getName().startsWith("flink-clients")
                                        && !p.toFile().getName().contains("test-utils"));

        assertThat(jarPaths).isNotEmpty();

        return jarPaths.iterator().next().toFile();
    }

    private static class DummyHttpDownloadHandler implements HttpHandler {

        final File file;

        DummyHttpDownloadHandler(File fileToDownload) {
            checkArgument(fileToDownload.exists(), "The file to be download not exists!");
            this.file = fileToDownload;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, file.length());
            FileUtils.copyFile(this.file, exchange.getResponseBody());
            exchange.close();
        }
    }

    private static class DummyFetcher extends ArtifactFetcher {

        int fetchCount = 0;

        @Override
        File fetch(String uri, Configuration flinkConfiguration, File targetDir) {
            ++fetchCount;
            return null;
        }
    }
}
