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

package org.apache.flink.client.cli;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.UnsupportedFileSystemSchemeException;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for running a job JAR given as a remote URI instead of a local path. */
class CliFrontendRemoteJarTest {

    private static final String JAR_NAME = "remote-job.jar";

    @TempDir private Path tmpDir;

    private final AtomicInteger downloads = new AtomicInteger();

    private HttpServer httpServer;

    private Configuration configuration;

    @BeforeAll
    static void init() {
        CliFrontendTestUtils.pipeSystemOutToNull();
    }

    @AfterAll
    static void shutdown() {
        CliFrontendTestUtils.restoreSystemOut();
    }

    @BeforeEach
    void setup() throws Exception {
        byte[] jar = Files.readAllBytes(Paths.get(getTestJarPath()));
        httpServer =
                HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.createContext(
                "/" + JAR_NAME,
                exchange -> {
                    downloads.incrementAndGet();
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, jar.length);
                    try (OutputStream body = exchange.getResponseBody()) {
                        body.write(jar);
                    }
                });
        httpServer.start();

        configuration = new Configuration();
        configuration.set(ArtifactFetchOptions.RAW_HTTP_ENABLED, true);
        configuration.set(CoreOptions.TMP_DIRS, tmpDir.toString());
    }

    @AfterEach
    void tearDown() {
        httpServer.stop(0);
    }

    @Test
    void testRemoteJarIsFetchedOnceAndDeleted() throws Exception {
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));
        ProgramOptions programOptions = programOptions(jarUri());

        try (PackagedProgram first = frontend.buildProgram(programOptions, configuration);
                PackagedProgram second = frontend.buildProgram(programOptions, configuration)) {
            assertThat(first.getMainClassName()).isEqualTo(TEST_JAR_MAIN_CLASS);
            assertThat(second.getMainClassName()).isEqualTo(TEST_JAR_MAIN_CLASS);
        }
        assertThat(downloads).hasValue(1);

        frontend.deleteFetchedJobJars();
        assertThat(tmpDir).isEmptyDirectory();
    }

    @Test
    void testRemoteJarIsFetchedAgainForTheNextAction() throws Exception {
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));
        ProgramOptions programOptions = programOptions(jarUri());

        frontend.buildProgram(programOptions, configuration).close();
        frontend.deleteFetchedJobJars();
        frontend.buildProgram(programOptions, configuration).close();
        frontend.deleteFetchedJobJars();

        // Fetched again, not reused: a changed remote JAR must not be shadowed by an old copy.
        assertThat(downloads).hasValue(2);
    }

    @Test
    void testRunFetchesRemoteJarAndDeletesItAfterSubmission() throws Exception {
        List<File> submittedJars = new ArrayList<>();
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI())) {
                    @Override
                    protected void executeProgram(
                            Configuration configuration, PackagedProgram program) {
                        for (URL jar : program.getJobJarAndDependencies()) {
                            File file = new File(jar.getPath());
                            assertThat(file).exists();
                            submittedJars.add(file);
                        }
                    }
                };

        frontend.run(new String[] {jarUri()});

        assertThat(submittedJars)
                .singleElement()
                .satisfies(jar -> assertThat(jar).hasName(JAR_NAME));
        assertThat(downloads).hasValue(1);
        assertThat(tmpDir).isEmptyDirectory();
    }

    @Test
    void testRemoteJarFetchFailureNamesTheUri() {
        configuration.set(ArtifactFetchOptions.RAW_HTTP_ENABLED, false);
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));
        ProgramOptions programOptions = programOptions(jarUri());

        assertThatThrownBy(() -> frontend.buildProgram(programOptions, configuration))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("Could not fetch JAR file " + jarUri())
                .hasMessageContaining("raw HTTP endpoints are disabled");
        frontend.deleteFetchedJobJars();
        assertThat(tmpDir).isEmptyDirectory();
    }

    @Test
    void testUnsupportedSchemeNamesTheMissingFileSystem() {
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));
        ProgramOptions programOptions = programOptions("unknown-fs://bucket/job.jar");

        assertThatThrownBy(() -> frontend.buildProgram(programOptions, configuration))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("Could not fetch JAR file unknown-fs://bucket/job.jar")
                .hasRootCauseInstanceOf(UnsupportedFileSystemSchemeException.class);
        frontend.deleteFetchedJobJars();
        assertThat(tmpDir).isEmptyDirectory();
    }

    @Test
    void testInfoFetchesRemoteJarAndDeletesIt() throws Exception {
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));

        frontend.info(new String[] {jarUri(), "-f", "true", "--arg", "suffix"});

        assertThat(downloads).hasValue(1);
        assertThat(tmpDir).isEmptyDirectory();
    }

    @Test
    void testLocalSchemeJarIsUsedInPlace() throws Exception {
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));
        String localJar = getTestJarPath();

        try (PackagedProgram program =
                frontend.buildProgram(programOptions("local://" + localJar), configuration)) {
            assertThat(program.getMainClassName()).isEqualTo(TEST_JAR_MAIN_CLASS);
        }
        assertThat(tmpDir).isEmptyDirectory();

        frontend.deleteFetchedJobJars();
        assertThat(new File(localJar)).exists();
    }

    @Test
    void testUriWithoutFileNameIsRejected() {
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));

        assertThatThrownBy(
                        () ->
                                frontend.buildProgram(
                                        programOptions("s3://bucket/jobs/"), configuration))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessage("JAR URI does not name a file: s3://bucket/jobs/");
        assertThat(tmpDir).isEmptyDirectory();
    }

    @Test
    void testUnusableTmpDirIsNamedInTheError() throws Exception {
        Path notADirectory = Files.createFile(tmpDir.resolve("not-a-directory"));
        configuration.set(CoreOptions.TMP_DIRS, notADirectory.toString());
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));

        assertThatThrownBy(() -> frontend.buildProgram(programOptions(jarUri()), configuration))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining(notADirectory.toString())
                .hasMessageContaining(CoreOptions.TMP_DIRS.key());
        assertThat(downloads).hasValue(0);
    }

    @Test
    void testOnlyUrisWithSchemeAreFetched() {
        assertThat(CliFrontend.hasUriScheme("s3://bucket/job.jar")).isTrue();
        assertThat(CliFrontend.hasUriScheme("hdfs:///jobs/job.jar")).isTrue();
        assertThat(CliFrontend.hasUriScheme("https://example.com/job.jar")).isTrue();
        assertThat(CliFrontend.hasUriScheme("file:///opt/job.jar")).isTrue();

        assertThat(CliFrontend.hasUriScheme("/opt/flink/job.jar")).isFalse();
        assertThat(CliFrontend.hasUriScheme("job.jar")).isFalse();
        assertThat(CliFrontend.hasUriScheme("C:\\flink\\job.jar")).isFalse();
        assertThat(CliFrontend.hasUriScheme("C:/flink/job.jar")).isFalse();
        assertThat(CliFrontend.hasUriScheme("/opt/my jobs/job.jar")).isFalse();
    }

    private String jarUri() {
        return String.format("http://127.0.0.1:%d/%s", httpServer.getAddress().getPort(), JAR_NAME);
    }

    private static ProgramOptions programOptions(String jarUri) {
        try {
            return ProgramOptions.create(
                    CliFrontendParser.parse(
                            CliFrontendParser.RUN_OPTIONS, new String[] {jarUri}, true));
        } catch (CliArgsException e) {
            throw new IllegalStateException(e);
        }
    }
}
