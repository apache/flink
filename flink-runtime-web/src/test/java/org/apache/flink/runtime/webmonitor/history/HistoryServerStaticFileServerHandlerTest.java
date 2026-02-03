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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.webmonitor.testutils.HttpUtils;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the HistoryServerStaticFileServerHandler. */
class HistoryServerStaticFileServerHandlerTest {

    @Test
    void testRespondWithFile(@TempDir Path tmpDir) throws Exception {
        final Path webDir = Files.createDirectory(tmpDir.resolve("webDir"));
        final Path uploadDir = Files.createDirectory(tmpDir.resolve("uploadDir"));
        final Path tempDir = Files.createDirectory(tmpDir.resolve("tmpDir"));
        final org.apache.flink.core.fs.Path tempDirFs =
                new org.apache.flink.core.fs.Path(tempDir.toUri());

        buildLocalCacheDirectory(webDir, "job123");
        buildRemoteStorageJobDirectory(tempDir, "jobabc");

        List<HistoryServer.RefreshLocation> refreshDirs = new ArrayList<>();
        FileSystem refreshFS = tempDirFs.getFileSystem();
        refreshDirs.add(new HistoryServer.RefreshLocation(tempDirFs, refreshFS));

        HistoryServerArchiveFetcher archiveFetcher =
                new HistoryServerArchiveFetcher(
                        refreshDirs,
                        webDir.toFile(),
                        (event) -> {},
                        true,
                        (archive, index) -> index <= 10, // Retain first 10 archives
                        5,
                        true,
                        3);

        Router router =
                new Router()
                        .addGet(
                                "/:*",
                                new HistoryServerStaticFileServerHandler(
                                        webDir.toFile(), true, archiveFetcher));
        WebFrontendBootstrap webUI =
                new WebFrontendBootstrap(
                        router,
                        LoggerFactory.getLogger(HistoryServerStaticFileServerHandlerTest.class),
                        uploadDir.toFile(),
                        null,
                        "localhost",
                        0,
                        new Configuration());

        int port = webUI.getServerPort();
        try {
            // verify that 404 message is returned when requesting a non-existent file
            Tuple2<Integer, String> notFound404 =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/hello");
            assertThat(notFound404.f0).isEqualTo(404);
            assertThat(notFound404.f1).contains("not found");

            // verify that we are able to fetch job files from the local cache
            Tuple2<Integer, String> foundFromCache =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/jobs/job123");
            assertThat(foundFromCache.f0).isEqualTo(200);

            // verify that we are able to fetch job files from the remote file system when the
            // file is not present in the local cache
            Tuple2<Integer, String> foundFromRemoteCache =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/jobs/jobabc");
            assertThat(foundFromRemoteCache.f0).isEqualTo(200);

            // verify that if a job does not exist either in the remote or local cache
            // that a 404 message is returned
            Tuple2<Integer, String> jobNotFound404 =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/jobs/jobfoo");
            assertThat(jobNotFound404.f0).isEqualTo(404);

            // verify that a) a file can be loaded using the ClassLoader and b) that the
            // HistoryServer
            // index_hs.html is injected
            Tuple2<Integer, String> index =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/index.html");
            assertThat(index.f0).isEqualTo(200);
            assertThat(index.f1).contains("Apache Flink Web Dashboard");

            // verify that index.html is appended if the request path ends on '/'
            Tuple2<Integer, String> index2 =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/");
            assertThat(index2).isEqualTo(index);

            // verify that a 405 message is returned when requesting a directory
            Files.createDirectory(webDir.resolve("dir.json"));
            Tuple2<Integer, String> dirNotFound =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/dir");
            assertThat(dirNotFound.f0).isEqualTo(405);
            assertThat(dirNotFound.f1).contains("not found");

            // verify that a 403 message is returned when requesting a file outside the webDir
            Files.createFile(tmpDir.resolve("secret"));
            Tuple2<Integer, String> dirOutsideDirectory =
                    HttpUtils.getFromHTTP("http://localhost:" + port + "/../secret");
            assertThat(dirOutsideDirectory.f0).isEqualTo(403);
            assertThat(dirOutsideDirectory.f1).contains("Forbidden");
        } finally {
            webUI.shutdown();
        }
    }

    private void buildLocalCacheDirectory(Path cacheDir, String jobID) throws IOException {
        File localCachePath = new File(cacheDir.toFile(), "jobs/" + jobID + ".json");
        localCachePath.getParentFile().mkdirs();
        Files.createFile(localCachePath.toPath());
    }

    private void buildRemoteStorageJobDirectory(Path refreshDir, String jobID) throws IOException {
        String json =
                "{\n"
                        + "   \"archive\": [\n"
                        + "      {\n"
                        + "         \"path\": \"/jobs/"
                        + jobID
                        + "\",\n"
                        + "         \"json\": \"{\\\"jid\\\":\\\""
                        + jobID
                        + "\\\",\\\"name\\\":\\\"WordCount\\\",\\\"isStoppable\\\":false,"
                        + "\\\"state\\\":\\\"FINISHED\\\",\\\"start-time\\\":1705527079656,\\\"end-time\\\":1705527080059,"
                        + "\\\"duration\\\":403,\\\"maxParallelism\\\":-1,\\\"now\\\":1705527080104,\\\"timestamps\\\":{\\\"FAILED\\\":"
                        + "0,\\\"FINISHED\\\":1705527080059,\\\"CANCELLING\\\":0,\\\"CANCELED\\\":0,\\\"INITIALIZING\\\":"
                        + "1705527079656,\\\"CREATED\\\":1705527079708,\\\"RUNNING\\\":1705527079763,\\\"RESTARTING\\\":"
                        + "0,\\\"SUSPENDED\\\":0,\\\"FAILING\\\":0,\\\"RECONCILING\\\":0},\\\"vertices\\\":[{\\\"id\\\":"
                        + "\\\"cbc357ccb763df2852fee8c4fc7d55f2\\\",\\\"name\\\":\\\"Source: in-memory-input -> tokenizer\\\","
                        + "\\\"maxParallelism\\\":128,\\\"parallelism\\\":1,\\\"status\\\":\\\"FINISHED\\\","
                        + "\\\"start-time\\\":1705527079881,\\\"end-time\\\":1705527080046,\\\"duration\\\":165,"
                        + "\\\"tasks\\\":{\\\"CANCELED\\\":0,\\\"FAILED\\\":0,\\\"FINISHED\\\":1,\\\"DEPLOYING\\\":"
                        + "0,\\\"RUNNING\\\":0,\\\"INITIALIZING\\\":0,\\\"SCHEDULED\\\":0,\\\"CANCELING\\\":0,"
                        + "\\\"RECONCILING\\\":0,\\\"CREATED\\\":0},\\\"metrics\\\":{\\\"read-bytes\\\":0,"
                        + "\\\"read-bytes-complete\\\":true,\\\"write-bytes\\\":4047,\\\"write-bytes-complete\\\":true,"
                        + "\\\"read-records\\\":0,\\\"read-records-complete\\\":true,\\\"write-records\\\":287,"
                        + "\\\"write-records-complete\\\":true,\\\"accumulated-backpressured-time\\\":0,"
                        + "\\\"accumulated-idle-time\\\":0,\\\"accumulated-busy-time\\\":\\\"NaN\\\"}},{\\\"id\\\":"
                        + "\\\""
                        + jobID
                        + "\\\",\\\"name\\\":\\\"counter -> Sink: print-sink\\\",\\\"maxParallelism\\\":"
                        + "128,\\\"parallelism\\\":1,\\\"status\\\":\\\"FINISHED\\\",\\\"start-time\\\":"
                        + "1705527079885,\\\"end-time\\\":1705527080057,\\\"duration\\\":172,\\\"tasks\\\":{\\\"CANCELED\\\":"
                        + "0,\\\"FAILED\\\":0,\\\"FINISHED\\\":1,\\\"DEPLOYING\\\":0,\\\"RUNNING\\\":0,\\\"INITIALIZING\\\":"
                        + "0,\\\"SCHEDULED\\\":0,\\\"CANCELING\\\":0,\\\"RECONCILING\\\":0,\\\"CREATED\\\":0},\\\"metrics\\\":"
                        + "{\\\"read-bytes\\\":4060,\\\"read-bytes-complete\\\":true,\\\"write-bytes\\\":0,"
                        + "\\\"write-bytes-complete\\\":true,\\\"read-records\\\":287,\\\"read-records-complete\\\":true,"
                        + "\\\"write-records\\\":0,\\\"write-records-complete\\\":true,\\\"accumulated-backpressured-time\\\":"
                        + "0,\\\"accumulated-idle-time\\\":1,\\\"accumulated-busy-time\\\":15.0}}],\\\"status-counts\\\":{\\\"CANCELED\\\":"
                        + "0,\\\"FAILED\\\":0,\\\"FINISHED\\\":2,\\\"DEPLOYING\\\":0,\\\"RUNNING\\\":0,\\\"INITIALIZING\\\":0,"
                        + "\\\"SCHEDULED\\\":0,\\\"CANCELING\\\":0,\\\"RECONCILING\\\":0,\\\"CREATED\\\":0},\\\"plan\\\":{\\\"jid\\\":"
                        + "\\\""
                        + jobID
                        + "\\\",\\\"name\\\":\\\"WordCount\\\",\\\"type\\\":\\\"STREAMING\\\",\\\"nodes\\\":[{\\\"id\\\":"
                        + "\\\""
                        + jobID
                        + "\\\",\\\"parallelism\\\":1,\\\"operator\\\":\\\"\\\",\\\"operator_strategy\\\":\\\"\\\","
                        + "\\\"description\\\":\\\"counter<br/>+- Sink: print-sink<br/>\\\",\\\"inputs\\\":[{\\\"num\\\":0,"
                        + "\\\"id\\\":\\\"cbc357ccb763df2852fee8c4fc7d55f2\\\",\\\"ship_strategy\\\":\\\"HASH\\\","
                        + "\\\"exchange\\\":\\\"pipelined_bounded\\\"}],\\\"optimizer_properties\\\":{}},{\\\"id\\\":"
                        + "\\\"cbc357ccb763df2852fee8c4fc7d55f2\\\",\\\"parallelism\\\":1,\\\"operator\\\":\\\"\\\","
                        + "\\\"operator_strategy\\\":\\\"\\\",\\\"description\\\":\\\"Source: in-memory-input<br/>+- tokenizer"
                        + "<br/>\\\",\\\"optimizer_properties\\\":{}}]}\"\n"
                        + "      }\n"
                        + "   ]\n"
                        + "}";
        Path remoteJobArchive = Files.createFile(refreshDir.resolve(jobID));
        FileUtils.writeStringToFile(remoteJobArchive.toFile(), json);
    }
}
