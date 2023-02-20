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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.RecoverableWriter.ResumeRecoverable;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Test for {@link org.apache.hadoop.fs.viewfs.ViewFileSystem} support. */
class HadoopViewFileSystemTruncateTest {

    @TempDir static File tempFolder;

    private final FileSystemTestHelper fileSystemTestHelper = new FileSystemTestHelper("/tests");

    private static MiniDFSCluster hdfsCluster;
    private static FileSystem fHdfs;
    private static org.apache.flink.core.fs.FileSystem fSystem;

    private Configuration fsViewConf;
    private FileSystem fsTarget;
    private Path targetTestRoot;

    @BeforeAll
    static void testHadoopVersion() {
        assumeThat(HadoopUtils.isMinHadoopVersion(2, 7)).isTrue();
    }

    @BeforeAll
    static void verifyOS() {
        assumeThat(OperatingSystem.isWindows())
                .describedAs("HDFS cluster cannot be started on Windows without extensions.")
                .isFalse();
    }

    @BeforeAll
    static void createHDFS() throws Exception {
        final File baseDir = tempFolder;

        final Configuration hdConf = new Configuration();
        hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

        final MiniDFSCluster.Builder builder =
                new MiniDFSCluster.Builder(hdConf)
                        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(1));
        hdfsCluster = builder.build();
        hdfsCluster.waitClusterUp();

        fHdfs = hdfsCluster.getFileSystem(0);
    }

    @BeforeEach
    void setUp() throws Exception {
        fsTarget = fHdfs;
        targetTestRoot = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);

        fsTarget.delete(targetTestRoot, true);
        fsTarget.mkdirs(targetTestRoot);

        fsViewConf = ViewFileSystemTestSetup.createConfig();
        setupMountPoints();
        FileSystem fsView = FileSystem.get(FsConstants.VIEWFS_URI, fsViewConf);
        fSystem = new HadoopFileSystem(fsView);
    }

    private void setupMountPoints() {
        Path mountOnNn1 = new Path("/mountOnNn1");
        ConfigUtil.addLink(fsViewConf, mountOnNn1.toString(), targetTestRoot.toUri());
    }

    @AfterAll
    static void shutdownCluster() {
        hdfsCluster.shutdown();
    }

    @AfterEach
    void tearDown() throws Exception {
        fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
    }

    @Test
    void testViewFileSystemRecoverWorks() throws IOException {

        final org.apache.flink.core.fs.Path testPath =
                new org.apache.flink.core.fs.Path(fSystem.getUri() + "mountOnNn1/test-1");
        final String expectedContent = "test_line";

        final RecoverableWriter writer = fSystem.createRecoverableWriter();
        final RecoverableFsDataOutputStream streamUnderTest =
                getOpenStreamToFileWithContent(writer, testPath, expectedContent);

        final ResumeRecoverable resumeRecover = streamUnderTest.persist();

        final RecoverableFsDataOutputStream recover = writer.recover(resumeRecover);

        final RecoverableWriter.CommitRecoverable committable =
                recover.closeForCommit().getRecoverable();

        final RecoverableWriter recoveredWriter = fSystem.createRecoverableWriter();
        recoveredWriter.recoverForCommit(committable).commitAfterRecovery();

        verifyFileContent(testPath, expectedContent);
    }

    private RecoverableFsDataOutputStream getOpenStreamToFileWithContent(
            final RecoverableWriter writerUnderTest,
            final org.apache.flink.core.fs.Path path,
            final String expectedContent)
            throws IOException {
        final byte[] content = expectedContent.getBytes(UTF_8);

        final RecoverableFsDataOutputStream streamUnderTest = writerUnderTest.open(path);
        streamUnderTest.write(content);
        return streamUnderTest;
    }

    private static void verifyFileContent(
            final org.apache.flink.core.fs.Path testPath, final String expectedContent)
            throws IOException {
        try (FSDataInputStream in = fSystem.open(testPath);
                InputStreamReader ir = new InputStreamReader(in, UTF_8);
                BufferedReader reader = new BufferedReader(ir)) {
            final String line = reader.readLine();
            assertThat(line).isEqualTo(expectedContent);
        }
    }
}
