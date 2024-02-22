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

import org.apache.flink.util.OperatingSystem;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.yarn.YarnTestUtils.generateFilesInDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for verifying file staging during submission to YARN works. */
public class YarnFileStageTest {

    private static final String LOCAL_RESOURCE_DIRECTORY = "stage_test";

    @TempDir public File tempFolder;

    private static MiniDFSCluster hdfsCluster;

    private static Path hdfsRootPath;

    private org.apache.hadoop.conf.Configuration hadoopConfig;

    // ------------------------------------------------------------------------
    //  Test setup and shutdown
    // ------------------------------------------------------------------------

    @BeforeAll
    static void createHDFS(@TempDir File hdfsTempDir) throws Exception {
        assumeThat(!OperatingSystem.isWindows()).isTrue();

        org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();
        hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsTempDir.getAbsolutePath());

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
        hdfsCluster = builder.build();
        hdfsRootPath = new Path(hdfsCluster.getURI());
    }

    @AfterAll
    static void destroyHDFS() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
        }
        hdfsCluster = null;
        hdfsRootPath = null;
    }

    @BeforeEach
    void initConfig() {
        hadoopConfig = new org.apache.hadoop.conf.Configuration();
        hadoopConfig.set(
                org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, hdfsRootPath.toString());
    }

    /**
     * Verifies that nested directories are properly copied with a <tt>hdfs://</tt> file system
     * (from a <tt>file:///absolute/path</tt> source path).
     */
    @Test
    public void testCopyFromLocalRecursiveWithScheme() throws Exception {
        final FileSystem targetFileSystem = hdfsRootPath.getFileSystem(hadoopConfig);
        final Path targetDir = targetFileSystem.getWorkingDirectory();

        testRegisterMultipleLocalResources(
                targetFileSystem, targetDir, LOCAL_RESOURCE_DIRECTORY, tempFolder, true, false);
    }

    /**
     * Verifies that nested directories are properly copied with a <tt>hdfs://</tt> file system
     * (from a <tt>/absolute/path</tt> source path).
     */
    @Test
    public void testCopyFromLocalRecursiveWithoutScheme() throws Exception {
        final FileSystem targetFileSystem = hdfsRootPath.getFileSystem(hadoopConfig);
        final Path targetDir = targetFileSystem.getWorkingDirectory();

        testRegisterMultipleLocalResources(
                targetFileSystem, targetDir, LOCAL_RESOURCE_DIRECTORY, tempFolder, false, false);
    }

    /** Verifies that a single file is properly copied. */
    @Test
    public void testCopySingleFileFromLocal()
            throws IOException, URISyntaxException, InterruptedException {
        final FileSystem targetFileSystem = hdfsRootPath.getFileSystem(hadoopConfig);
        final Path targetDir = targetFileSystem.getWorkingDirectory();

        testCopySingleFileFromLocal(
                targetFileSystem, targetDir, LOCAL_RESOURCE_DIRECTORY, tempFolder);
    }

    /** Verifies that a symbolic path is properly copied. */
    @Test
    public void testCopySymbolicPathFromLocal()
            throws IOException, URISyntaxException, InterruptedException {
        final FileSystem targetFileSystem = hdfsRootPath.getFileSystem(hadoopConfig);
        final Path targetDir = targetFileSystem.getWorkingDirectory();

        testCopySymbolicPathFromLocal(
                targetFileSystem, targetDir, LOCAL_RESOURCE_DIRECTORY, tempFolder);
    }

    @Test
    public void testRegisterMultipleLocalResourcesWithRemoteFiles() throws Exception {
        final FileSystem targetFileSystem = hdfsRootPath.getFileSystem(hadoopConfig);
        final Path targetDir = targetFileSystem.getWorkingDirectory();

        testRegisterMultipleLocalResources(
                targetFileSystem, targetDir, LOCAL_RESOURCE_DIRECTORY, tempFolder, true, true);
    }

    /**
     * Verifies that nested directories are properly copied and registered with the given filesystem
     * and paths.
     *
     * @param targetFileSystem file system of the target path
     * @param targetDir target path (URI like <tt>hdfs://...</tt>)
     * @param localResourceDirectory the directory that localResource are uploaded to
     * @param srcDir JUnit temporary folder rule to create the source directory with
     * @param addSchemeToLocalPath whether add the <tt>file://</tt> scheme to the local path to copy
     *     from
     * @param useRemoteFiles whether register the local resource with remote files
     */
    static void testRegisterMultipleLocalResources(
            FileSystem targetFileSystem,
            Path targetDir,
            String localResourceDirectory,
            File srcDir,
            boolean addSchemeToLocalPath,
            boolean useRemoteFiles)
            throws Exception {

        // directory must not yet exist
        assertThat(targetFileSystem.exists(targetDir)).isFalse();

        final HashMap<String /* (relative) path */, /* contents */ String> srcFiles =
                new HashMap<>(4);

        // create and fill source files
        srcFiles.put("1", "Hello 1");
        srcFiles.put("2", "Hello 2");
        srcFiles.put("nested/3", "Hello nested/3");
        srcFiles.put("nested/4/5", "Hello nested/4/5");
        srcFiles.put("test.jar", "JAR Content");

        generateFilesInDirectory(srcDir, srcFiles);

        final Path srcPath;
        if (useRemoteFiles) {
            srcPath = new Path(hdfsRootPath.toString() + "/tmp/remoteFiles");
            hdfsCluster
                    .getFileSystem()
                    .copyFromLocalFile(new Path(srcDir.getAbsolutePath()), srcPath);
        } else {
            if (addSchemeToLocalPath) {
                srcPath = new Path("file://" + srcDir.getAbsolutePath());
            } else {
                srcPath = new Path(srcDir.getAbsolutePath());
            }
        }

        // copy the created directory recursively:
        try {
            final List<Path> remotePaths = new ArrayList<>();

            final ApplicationId applicationId = ApplicationId.newInstance(0, 0);
            final YarnApplicationFileUploader uploader =
                    YarnApplicationFileUploader.from(
                            targetFileSystem,
                            targetDir,
                            Collections.emptyList(),
                            applicationId,
                            DFSConfigKeys.DFS_REPLICATION_DEFAULT);

            final List<String> classpath =
                    uploader.registerMultipleLocalResources(
                            Collections.singletonList(srcPath),
                            localResourceDirectory,
                            LocalResourceType.FILE);

            final Path basePath = new Path(localResourceDirectory, srcPath.getName());
            final Path nestedPath = new Path(basePath, "nested");
            assertThat(classpath)
                    .containsExactlyInAnyOrder(
                            basePath.toString(),
                            nestedPath.toString(),
                            new Path(nestedPath, "4").toString(),
                            new Path(basePath, "test.jar").toString());

            final Map<String, LocalResource> localResources =
                    uploader.getRegisteredLocalResources();
            assertThat(localResources).hasSameSizeAs(srcFiles);

            final Path workDir =
                    localResources
                            .get(
                                    new Path(
                                                    localResourceDirectory,
                                                    new Path(srcPath.getName(), "1"))
                                            .toString())
                            .getResource()
                            .toPath()
                            .getParent();

            verifyDirectoryRecursive(targetFileSystem, workDir, srcFiles);

        } finally {
            // clean up
            targetFileSystem.delete(targetDir, true);
        }
    }

    /**
     * Verifies a single file is properly uploaded.
     *
     * @param targetFileSystem file system of the target path
     * @param targetDir target path (URI like <tt>hdfs://...</tt>)
     * @param localResourceDirectory the directory that localResource are uploaded to
     * @param srcDir JUnit temporary folder rule to create the source directory with
     * @throws IOException if error occurs when accessing the file system
     * @throws URISyntaxException if the format of url has errors when converting a given url to
     *     hadoop path
     */
    private static void testCopySingleFileFromLocal(
            FileSystem targetFileSystem, Path targetDir, String localResourceDirectory, File srcDir)
            throws IOException, InterruptedException, URISyntaxException {

        final String localFile = "local.jar";
        final String localFileContent = "Local Jar Content";

        final HashMap<String /* (relative) path */, /* contents */ String> srcFiles =
                new HashMap<>(4);
        srcFiles.put(localFile, localFileContent);

        generateFilesInDirectory(srcDir, srcFiles);
        try {
            final List<Path> remotePaths = new ArrayList<>();

            final ApplicationId applicationId = ApplicationId.newInstance(0, 0);
            final YarnApplicationFileUploader uploader =
                    YarnApplicationFileUploader.from(
                            targetFileSystem,
                            targetDir,
                            Collections.emptyList(),
                            applicationId,
                            DFSConfigKeys.DFS_REPLICATION_DEFAULT);

            final List<String> classpath =
                    uploader.registerMultipleLocalResources(
                            Collections.singletonList(
                                    new Path(srcDir.getAbsolutePath(), localFile)),
                            localResourceDirectory,
                            LocalResourceType.FILE);

            assertThat(classpath)
                    .containsExactly(new Path(localResourceDirectory, localFile).toString());

            final Map<String, LocalResource> localResources =
                    uploader.getRegisteredLocalResources();
            final Path workDir =
                    localResources
                            .get(new Path(localResourceDirectory, localFile).toString())
                            .getResource()
                            .toPath()
                            .getParent();
            verifyDirectoryRecursive(targetFileSystem, workDir, srcFiles);
        } finally {
            targetFileSystem.delete(targetDir, true);
        }
    }

    /**
     * Verifies that a symbolic path is properly uploaded.
     *
     * @param targetFileSystem file system of the target path
     * @param targetDir target path (URI like <tt>hdfs://...</tt>)
     * @param localResourceDirectory the directory that localResource are uploaded to
     * @param srcDir JUnit temporary folder rule to create the source directory with
     * @throws IOException if error occurs when accessing the file system
     * @throws URISyntaxException if the format of url has errors when converting a given url to
     *     hadoop path
     */
    private static void testCopySymbolicPathFromLocal(
            FileSystem targetFileSystem, Path targetDir, String localResourceDirectory, File srcDir)
            throws IOException, InterruptedException, URISyntaxException {

        final String srcPath = srcDir.getAbsolutePath();

        final String localSymbolicFile = "local.lnk";

        final HashMap<String /* (relative) path */, /* contents */ String> srcFiles =
                new HashMap<>(4);
        srcFiles.put("1", "Hello file");
        srcFiles.put("nested/local.jar", "Local Jar Content");

        generateFilesInDirectory(srcDir, srcFiles);

        Files.createSymbolicLink(
                Paths.get(srcPath, localSymbolicFile), Paths.get(srcPath, "nested"));

        try {

            final ApplicationId applicationId = ApplicationId.newInstance(0, 0);
            final YarnApplicationFileUploader uploader =
                    YarnApplicationFileUploader.from(
                            targetFileSystem,
                            targetDir,
                            Collections.emptyList(),
                            applicationId,
                            DFSConfigKeys.DFS_REPLICATION_DEFAULT);

            final List<String> classpath =
                    uploader.registerMultipleLocalResources(
                            Lists.newArrayList(
                                    new Path(srcPath, localSymbolicFile), new Path(srcPath, "1")),
                            localResourceDirectory,
                            LocalResourceType.FILE);

            // resource directories go first
            assertThat(classpath)
                    .containsExactlyInAnyOrder(
                            new Path(localResourceDirectory).toString(),
                            new Path(localResourceDirectory, "nested/local.jar").toString());

            final Map<String, LocalResource> localResources =
                    uploader.getRegisteredLocalResources();
            final Path workDir =
                    localResources
                            .get(new Path(localResourceDirectory, "1").toString())
                            .getResource()
                            .toPath()
                            .getParent();
            verifyDirectoryRecursive(targetFileSystem, workDir, srcFiles);
        } finally {
            targetFileSystem.delete(targetDir, true);
        }
    }

    /**
     * Verifies the content and name of file in the directory {@code worDir} are same with {@code
     * expectedFiles}.
     *
     * @param targetFileSystem the filesystem type of {@code workDir}
     * @param workDir the directory verified
     * @param expectedFiles the expected name and content of the files
     * @throws IOException if error occurs when visiting the {@code workDir}
     * @throws InterruptedException if the sleep is interrupted.
     */
    private static void verifyDirectoryRecursive(
            FileSystem targetFileSystem, Path workDir, Map<String, String> expectedFiles)
            throws IOException, InterruptedException {

        final HashMap<String /* (relative) path */, /* contents */ String> targetFiles =
                new HashMap<>();
        final RemoteIterator<LocatedFileStatus> targetFilesIterator =
                targetFileSystem.listFiles(workDir, true);
        final int workDirPrefixLength =
                workDir.toString().length() + 1; // one more for the concluding "/"
        while (targetFilesIterator.hasNext()) {
            final LocatedFileStatus targetFile = targetFilesIterator.next();

            int retries = 5;
            do {
                try (FSDataInputStream in = targetFileSystem.open(targetFile.getPath())) {
                    String absolutePathString = targetFile.getPath().toString();
                    String relativePath = absolutePathString.substring(workDirPrefixLength);
                    targetFiles.put(relativePath, in.readUTF());

                    assertThat(in.read())
                            .as("extraneous data in file " + relativePath)
                            .isEqualTo(-1);
                    break;
                } catch (FileNotFoundException e) {
                    // For S3, read-after-write may be eventually consistent, i.e. when trying
                    // to access the object before writing it; see
                    // https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel
                    // -> try again a bit later
                    Thread.sleep(50);
                }
            } while ((retries--) > 0);
        }
        assertThat(targetFiles).isEqualTo(expectedFiles);
    }
}
