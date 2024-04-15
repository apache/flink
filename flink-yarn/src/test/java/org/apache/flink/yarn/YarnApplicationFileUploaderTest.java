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

package org.apache.flink.yarn;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.IOUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.apache.flink.yarn.YarnTestUtils.generateFilesInDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link YarnApplicationFileUploader}. */
class YarnApplicationFileUploaderTest {

    @Test
    void testRegisterProvidedLocalResources(@TempDir File flinkLibDir) throws IOException {
        final Map<String, String> libJars = getLibJars();

        generateFilesInDirectory(flinkLibDir, libJars);

        try (final YarnApplicationFileUploader yarnApplicationFileUploader =
                YarnApplicationFileUploader.from(
                        FileSystem.get(new YarnConfiguration()),
                        new Path(flinkLibDir.toURI()),
                        Collections.singletonList(new Path(flinkLibDir.toURI())),
                        ApplicationId.newInstance(0, 0),
                        DFSConfigKeys.DFS_REPLICATION_DEFAULT)) {

            yarnApplicationFileUploader.registerProvidedLocalResources();

            final Set<String> registeredResources =
                    yarnApplicationFileUploader.getRegisteredLocalResources().keySet();

            assertThat(registeredResources).containsExactlyInAnyOrderElementsOf(libJars.keySet());
        }
    }

    @Test
    void testRegisterProvidedLocalResourcesWithParentDir(@TempDir File flinkLibDir)
            throws IOException {
        final String xmlContent = "XML Content";
        final Map<String, String> xmlResources =
                ImmutableMap.of(
                        "conf/hive-site.xml", xmlContent, "conf/ivysettings.xml", xmlContent);
        generateFilesInDirectory(flinkLibDir, xmlResources);

        try (final YarnApplicationFileUploader yarnApplicationFileUploader =
                YarnApplicationFileUploader.from(
                        FileSystem.get(new YarnConfiguration()),
                        new Path(flinkLibDir.toURI()),
                        Collections.singletonList(new Path(flinkLibDir.toURI())),
                        ApplicationId.newInstance(0, 0),
                        DFSConfigKeys.DFS_REPLICATION_DEFAULT)) {

            List<String> classPath = yarnApplicationFileUploader.registerProvidedLocalResources();
            List<String> expectedClassPathEntries = Arrays.asList("conf");

            assertThat(classPath).containsExactlyInAnyOrderElementsOf(expectedClassPathEntries);
        }
    }

    @Test
    void testRegisterProvidedLocalResourcesWithDuplication(@TempDir java.nio.file.Path tempDir)
            throws IOException {
        final File flinkLibDir1 =
                Files.createTempDirectory(tempDir, UUID.randomUUID().toString()).toFile();
        final File flinkLibDir2 =
                Files.createTempDirectory(tempDir, UUID.randomUUID().toString()).toFile();

        generateFilesInDirectory(flinkLibDir1, getLibJars());
        generateFilesInDirectory(flinkLibDir2, getLibJars());

        final FileSystem fileSystem = FileSystem.get(new YarnConfiguration());
        try {
            assertThrows(
                    "Two files with the same filename exist in the shared libs",
                    RuntimeException.class,
                    () ->
                            YarnApplicationFileUploader.from(
                                    fileSystem,
                                    new Path(tempDir.toFile().toURI()),
                                    Arrays.asList(
                                            new Path(flinkLibDir1.toURI()),
                                            new Path(flinkLibDir2.toURI())),
                                    ApplicationId.newInstance(0, 0),
                                    DFSConfigKeys.DFS_REPLICATION_DEFAULT));
        } finally {
            IOUtils.closeQuietly(fileSystem);
        }
    }

    @Test
    void testRegisterProvidedLocalResourcesWithNotAllowedUsrLib(@TempDir File flinkHomeDir)
            throws IOException {
        final File flinkLibDir = new File(flinkHomeDir, "lib");
        final File flinkUsrLibDir = new File(flinkHomeDir, "usrlib");
        final Map<String, String> libJars = getLibJars();
        final Map<String, String> usrLibJars = getUsrLibJars();

        generateFilesInDirectory(flinkLibDir, libJars);
        generateFilesInDirectory(flinkUsrLibDir, usrLibJars);
        final List<Path> providedLibDirs = new ArrayList<>();
        providedLibDirs.add(new Path(flinkLibDir.toURI()));
        providedLibDirs.add(new Path(flinkUsrLibDir.toURI()));

        assertThatThrownBy(
                        () ->
                                YarnApplicationFileUploader.from(
                                        FileSystem.get(new YarnConfiguration()),
                                        new Path(flinkHomeDir.getPath()),
                                        providedLibDirs,
                                        ApplicationId.newInstance(0, 0),
                                        DFSConfigKeys.DFS_REPLICATION_DEFAULT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Provided lib directories, configured via %s, should not include %s.",
                        YarnConfigOptions.PROVIDED_LIB_DIRS.key(),
                        ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);
    }

    @Test
    void testUploadLocalFileWithoutScheme(@TempDir File flinkHomeDir) throws IOException {
        final MockLocalFileSystem fileSystem = new MockLocalFileSystem();
        final File tempFile = File.createTempFile(UUID.randomUUID().toString(), "", flinkHomeDir);
        final Path pathWithoutScheme = new Path(tempFile.getAbsolutePath());

        try (final YarnApplicationFileUploader yarnApplicationFileUploader =
                YarnApplicationFileUploader.from(
                        fileSystem,
                        new Path(flinkHomeDir.getPath()),
                        Collections.emptyList(),
                        ApplicationId.newInstance(0, 0),
                        DFSConfigKeys.DFS_REPLICATION_DEFAULT)) {

            yarnApplicationFileUploader.uploadLocalFileToRemote(pathWithoutScheme, "");
            assertThat(fileSystem.getCopiedPaths())
                    .hasSize(1)
                    .allMatch(path -> "file".equals(path.toUri().getScheme()));
        }
    }

    private static Map<String, String> getLibJars() {
        final HashMap<String, String> libJars = new HashMap<>(4);
        final String jarContent = "JAR Content";

        libJars.put("flink-dist.jar", jarContent);
        libJars.put("log4j.jar", jarContent);
        libJars.put("flink-table.jar", jarContent);

        return libJars;
    }

    private static Map<String, String> getUsrLibJars() {
        final HashMap<String, String> usrLibJars = new HashMap<>();
        final String jarContent = "JAR Content";

        usrLibJars.put("udf.jar", jarContent);

        return usrLibJars;
    }

    private static class MockLocalFileSystem extends LocalFileSystem {

        private final List<Path> copiedPaths = new LinkedList<>();

        @Override
        public void copyFromLocalFile(Path src, Path dst) {
            copyFromLocalFile(false, src, dst);
        }

        @Override
        public void copyFromLocalFile(boolean delSrc, Path src, Path dst) {
            copyFromLocalFile(delSrc, true, src, dst);
        }

        @Override
        public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) {
            copyFromLocalFile(delSrc, overwrite, new Path[] {src}, dst);
        }

        @Override
        public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) {
            Collections.addAll(copiedPaths, srcs[srcs.length - 1]);
        }

        public List<Path> getCopiedPaths() {
            return copiedPaths;
        }
    }
}
