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
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

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
        // Only the (empty) cache directory is left behind.
        assertThat(tmpDir).isDirectoryNotContaining(path -> !path.equals(cacheDir()));
        assertThat(cacheDir()).isEmptyDirectory();
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
    void testFileSystemJarIsCachedAcrossActions(@TempDir Path remoteDir) throws Exception {
        assumePosix();
        String uri = copyTestJarTo(remoteDir);
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));

        buildAndDelete(frontend, uri);
        Path cached = cachedJar();
        Object firstCopy = Files.readAttributes(cached, BasicFileAttributes.class).fileKey();
        buildAndDelete(frontend, uri);

        // Still the file moved into the cache by the first action: not fetched again.
        assertThat(Files.readAttributes(cached, BasicFileAttributes.class).fileKey())
                .isEqualTo(firstCopy);
        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(cacheDir())))
                .isEqualTo("rwx------");
    }

    @Test
    void testChangedFileSystemJarRefreshesTheCache(@TempDir Path remoteDir) throws Exception {
        assumePosix();
        String uri = copyTestJarTo(remoteDir);
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));

        buildAndDelete(frontend, uri);
        Object firstCopy = Files.readAttributes(cachedJar(), BasicFileAttributes.class).fileKey();
        Path remoteJar = remoteDir.resolve(JAR_NAME);
        Files.setLastModifiedTime(
                remoteJar,
                FileTime.fromMillis(Files.getLastModifiedTime(remoteJar).toMillis() + 60_000));
        buildAndDelete(frontend, uri);

        assertThat(Files.readAttributes(cachedJar(), BasicFileAttributes.class).fileKey())
                .isNotEqualTo(firstCopy);
    }

    @Test
    void testChangedLengthRefreshesTheCache(@TempDir Path remoteDir) throws Exception {
        assumePosix();
        String uri = copyTestJarTo(remoteDir);
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));

        buildAndDelete(frontend, uri);
        Path remoteJar = remoteDir.resolve(JAR_NAME);
        FileTime modificationTime = Files.getLastModifiedTime(remoteJar);
        long length = Files.size(remoteJar);
        addEntry(remoteJar, "added.txt");
        Files.setLastModifiedTime(remoteJar, modificationTime);
        assertThat(Files.size(remoteJar)).isNotEqualTo(length);
        buildAndDelete(frontend, uri);

        assertThat(cachedJar()).hasSameBinaryContentAs(remoteJar);
    }

    @Test
    void testJarWithoutModificationTimeIsNotCached(@TempDir Path remoteDir) throws Exception {
        assumePosix();
        String uri = copyTestJarTo(remoteDir);
        Files.setLastModifiedTime(remoteDir.resolve(JAR_NAME), FileTime.fromMillis(0));
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));

        buildAndDelete(frontend, uri);

        assertThat(cacheDir()).isEmptyDirectory();
        assertThat(tmpDir).isDirectoryNotContaining(path -> !path.equals(cacheDir()));
    }

    @Test
    void testCacheIsNotUsedWhenOthersCanAccessIt(@TempDir Path remoteDir) throws Exception {
        assumePosix();
        Files.createDirectory(
                cacheDir(),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")));
        Files.setPosixFilePermissions(cacheDir(), PosixFilePermissions.fromString("rwxr-xr-x"));
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));

        buildAndDelete(frontend, copyTestJarTo(remoteDir));

        assertThat(cacheDir()).isEmptyDirectory();
        assertThat(tmpDir).isDirectoryNotContaining(path -> !path.equals(cacheDir()));
    }

    @Test
    void testCacheIsNotUsedThroughASymlink(@TempDir Path remoteDir) throws Exception {
        assumePosix();
        Path elsewhere =
                Files.createDirectory(
                        tmpDir.resolve("elsewhere"),
                        PosixFilePermissions.asFileAttribute(
                                PosixFilePermissions.fromString("rwx------")));
        Files.createSymbolicLink(cacheDir(), elsewhere);
        CliFrontend frontend =
                new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));

        buildAndDelete(frontend, copyTestJarTo(remoteDir));

        assertThat(elsewhere).isEmptyDirectory();
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

    private static void assumePosix() {
        assumeThat(FileSystems.getDefault().supportedFileAttributeViews()).contains("posix");
    }

    private static String copyTestJarTo(Path dir) throws Exception {
        Path jar = Files.copy(Paths.get(getTestJarPath()), dir.resolve(JAR_NAME));
        return jar.toUri().toString();
    }

    private static void addEntry(Path jar, String name) throws Exception {
        try (FileSystem zip = FileSystems.newFileSystem(jar, (ClassLoader) null)) {
            Files.write(zip.getPath(name), name.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void buildAndDelete(CliFrontend frontend, String uri) throws Exception {
        try (PackagedProgram program = frontend.buildProgram(programOptions(uri), configuration)) {
            assertThat(program.getMainClassName()).isEqualTo(TEST_JAR_MAIN_CLASS);
        }
        frontend.deleteFetchedJobJars();
    }

    private Path cacheDir() {
        return tmpDir.resolve(JobJarCache.DIRECTORY_NAME);
    }

    private Path cachedJar() throws Exception {
        try (Stream<Path> entries = Files.list(cacheDir())) {
            List<Path> cached = entries.collect(Collectors.toList());
            assertThat(cached).hasSize(1);
            return cached.get(0).resolve(JAR_NAME);
        }
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
