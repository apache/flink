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

package org.apache.flink.kubernetes.artifact;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.util.Preconditions;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
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
import java.net.URL;
import java.nio.file.Path;
import java.util.HashMap;

/** Test for {@link ArtifactUtils}. */
public class ArtifactUtilsTest {
    private static final Logger LOG = LoggerFactory.getLogger(ArtifactUtilsTest.class);
    public static final String TEST_NAMESPACE = "flink-artifact-namespace-test";
    public static final String TEST_CLUSTER_ID = "flink-artifact-cluster-id-test";
    private Configuration configuration;
    @TempDir Path tempDir;

    @BeforeEach
    public void setup() {
        configuration = new Configuration();
        configuration.setString(
                KubernetesConfigOptions.KUBERNETES_USER_ARTIFACTS_BASE_DIR,
                tempDir.toAbsolutePath().toString());
        configuration.setString(KubernetesConfigOptions.NAMESPACE, TEST_NAMESPACE);
        configuration.setString(KubernetesConfigOptions.CLUSTER_ID, TEST_CLUSTER_ID);
    }

    @Test
    public void testGenerateJarDir() {
        String baseDir = ArtifactUtils.generateJarDir(configuration);
        String expectedDir =
                String.join(
                        File.separator,
                        new String[] {tempDir.toString(), TEST_NAMESPACE, TEST_CLUSTER_ID});
        Assertions.assertEquals(expectedDir, baseDir);
    }

    @Test
    public void testFilesystemFetch() throws Exception {
        File sourceFile = mockTheJarFile();
        File file =
                ArtifactUtils.fetch(
                        String.format("file://%s", sourceFile.toURI().getPath()),
                        configuration,
                        tempDir.toString());
        Assertions.assertTrue(file.exists());
        Assertions.assertEquals(tempDir.toString(), file.getParentFile().toString());
    }

    @Test
    public void testHttpFetch() throws Exception {
        HttpServer httpServer = null;
        try {
            httpServer = startHttpServer();
            File sourceFile = mockTheJarFile();
            httpServer.createContext(
                    "/download/major.jar", new DownloadFileHttpHandler(sourceFile));

            File file =
                    ArtifactUtils.fetch(
                            String.format(
                                    "http://127.0.0.1:%d/download/major.jar",
                                    httpServer.getAddress().getPort()),
                            new Configuration()
                                    .set(
                                            KubernetesConfigOptions
                                                    .KUBERNETES_USER_JAR_ARTIFACT_HTTP_HEADER,
                                            new HashMap<String, String>() {
                                                {
                                                    put("k1", "v1");
                                                }
                                            }),
                            tempDir.toString());
            Assertions.assertTrue(file.exists());
            Assertions.assertEquals(tempDir.toString(), file.getParent());
            Assertions.assertEquals("major.jar", file.getName());
        } finally {
            if (httpServer != null) {
                httpServer.stop(0);
            }
        }
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

    private File mockTheJarFile() {
        String className = String.format("%s.class", ArtifactUtilsTest.class.getSimpleName());
        URL url = ArtifactUtilsTest.class.getResource(className);
        Assertions.assertNotNull(url);
        return new File(url.getPath());
    }

    /** Handler to mock download file. */
    public static class DownloadFileHttpHandler implements HttpHandler {

        private final File file;
        private final String contentType = "application/octet-stream";

        public DownloadFileHttpHandler(File fileToDownload) {
            Preconditions.checkArgument(
                    fileToDownload.exists(), "The file to be download not exists!");
            this.file = fileToDownload;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Content-Type", contentType);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, file.length());
            FileUtils.copyFile(this.file, exchange.getResponseBody());
            exchange.close();
        }
    }
}
