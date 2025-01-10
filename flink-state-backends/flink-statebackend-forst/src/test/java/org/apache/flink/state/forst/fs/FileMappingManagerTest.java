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

package org.apache.flink.state.forst.fs;

import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.state.forst.fs.filemapping.FileMappingManager;
import org.apache.flink.state.forst.fs.filemapping.FileOwnershipDecider;
import org.apache.flink.state.forst.fs.filemapping.MappingEntry;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link FileMappingManager}. */
@ExtendWith(ParameterizedTestExtension.class)
public class FileMappingManagerTest {
    @TempDir static java.nio.file.Path tempDir;

    @Parameters(name = "reuseCp: {0}")
    public static List<Object[]> params() {
        return Arrays.asList(new Object[][] {{true}, {false}});
    }

    @Parameter public boolean reuseCp;

    FileOwnershipDecider createFileOwnershipDecider() {
        return new FileOwnershipDecider(RecoveryClaimMode.CLAIM);
    }

    private MappingEntry registerFile(FileMappingManager manager, Path filePath) {
        if (reuseCp) {
            return manager.registerReusedRestoredFile(
                    filePath.toString(), new FileStateHandle(filePath, 0), filePath);
        } else {
            return manager.createNewFile(filePath);
        }
    }

    @TestTemplate
    void testFileLink() throws IOException {
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(
                        localFS,
                        createFileOwnershipDecider(),
                        tempDir.toString(),
                        tempDir.toString());
        String src = tempDir + "/source";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();
        registerFile(fileMappingManager, new Path(src));
        String dst = tempDir.toString() + "/dst";
        fileMappingManager.link(src, dst);
        assertThat(fileMappingManager.mappingEntry(dst).getSourcePath())
                .isEqualTo(fileMappingManager.mappingEntry(src).getSourcePath());
    }

    @TestTemplate
    void testNestLink() throws IOException {
        // link b->a
        // link c->b
        // link d->c
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(
                        localFS,
                        createFileOwnershipDecider(),
                        tempDir.toString(),
                        tempDir.toString());
        String src = tempDir + "/a";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();
        registerFile(fileMappingManager, new Path(src));
        String dstB = tempDir.toString() + "/b";
        fileMappingManager.link(src, dstB);
        assertThat(fileMappingManager.mappingEntry(dstB).getSourcePath())
                .isEqualTo(fileMappingManager.mappingEntry(src).getSourcePath());
        assertThat(fileMappingManager.mappingEntry(dstB).getReferenceCount()).isEqualTo(2);

        String dstC = tempDir.toString() + "/c";
        fileMappingManager.link(dstB, dstC);
        assertThat(fileMappingManager.mappingEntry(dstC).getSourcePath())
                .isEqualTo(fileMappingManager.mappingEntry(src).getSourcePath());
        assertThat(fileMappingManager.mappingEntry(dstC).getReferenceCount()).isEqualTo(3);

        String dstD = tempDir.toString() + "/d";
        fileMappingManager.link(dstC, dstD);
        assertThat(fileMappingManager.mappingEntry(dstD).getSourcePath())
                .isEqualTo(fileMappingManager.mappingEntry(src).getSourcePath());
        assertThat(fileMappingManager.mappingEntry(dstC).getReferenceCount()).isEqualTo(4);

        assertThat(fileMappingManager.link(dstD, dstC)).isEqualTo(-1);
    }

    @TestTemplate
    void testFileDelete() throws IOException {
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(
                        localFS,
                        createFileOwnershipDecider(),
                        tempDir.toString(),
                        tempDir.toString());
        String src = tempDir + "/source";
        registerFile(fileMappingManager, new Path(src));
        Path srcFileRealPath = fileMappingManager.mappingEntry(src).getSourcePath();
        FSDataOutputStream os = localFS.create(srcFileRealPath, FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();
        String dst = tempDir.toString() + "/dst";
        fileMappingManager.link(src, dst);
        assertThat(localFS.exists(srcFileRealPath)).isTrue();

        // delete src
        fileMappingManager.deleteFileOrDirectory(new Path(src), false);
        assertThat(localFS.exists(srcFileRealPath)).isTrue();

        // delete dst
        fileMappingManager.deleteFileOrDirectory(new Path(dst), false);
        if (reuseCp) {
            assertThat(localFS.exists(srcFileRealPath)).isTrue();
        } else {
            assertThat(localFS.exists(srcFileRealPath)).isFalse();
        }
    }

    @TestTemplate
    void testDirectoryDelete() throws IOException {
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(
                        localFS,
                        createFileOwnershipDecider(),
                        tempDir.toString() + "/db",
                        tempDir.toString() + "/db");
        String testDir = tempDir + "/testDir";
        localFS.mkdirs(new Path(testDir));
        String src = testDir + "/source";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();
        registerFile(fileMappingManager, new Path(src));
        String dst = tempDir.toString() + "/dst";
        fileMappingManager.link(src, dst);

        // delete testDir
        fileMappingManager.deleteFileOrDirectory(new Path(testDir), true);
        assertThat(localFS.exists(new Path(src))).isTrue();
        assertThat(localFS.exists(new Path(testDir))).isTrue();

        // delete dst
        fileMappingManager.deleteFileOrDirectory(new Path(dst), false);
        assertThat(localFS.exists(new Path(src))).isFalse();
        assertThat(localFS.exists(new Path(testDir))).isFalse();
    }

    @TestTemplate
    void testDirectoryRename() throws IOException {
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(
                        localFS,
                        createFileOwnershipDecider(),
                        tempDir.toString() + "/db",
                        tempDir.toString() + "/db");
        String testDir = tempDir + "/testDir";
        localFS.mkdirs(new Path(testDir));
        String src = testDir + "/source";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();

        String linkedDirTmp = tempDir.toString() + "/linkedDir.tmp";
        localFS.mkdirs(new Path(linkedDirTmp));
        String linkedSrc = linkedDirTmp + "/source";
        registerFile(fileMappingManager, new Path(src));
        fileMappingManager.link(src, linkedSrc);

        String linkedDir = tempDir.toString() + "/linkedDir";
        // rename linkDir.tmp to linkedDir
        assertThat(fileMappingManager.renameFile(linkedDirTmp, linkedDir)).isEqualTo(true);
        linkedSrc = linkedDir + "/source";

        // delete src
        assertThat(fileMappingManager.deleteFileOrDirectory(new Path(src), false)).isEqualTo(true);
        assertThat(localFS.exists(new Path(testDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isFalse();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(src))).isTrue();

        // delete testDir
        fileMappingManager.deleteFileOrDirectory(new Path(testDir), true);
        assertThat(localFS.exists(new Path(testDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(src))).isTrue();

        // delete linkedSrc
        assertThat(fileMappingManager.deleteFileOrDirectory(new Path(linkedSrc), false))
                .isEqualTo(true);
        assertThat(localFS.exists(new Path(src))).isFalse();
        assertThat(localFS.exists(new Path(testDir))).isFalse();

        // delete linkedDir
        assertThat(fileMappingManager.deleteFileOrDirectory(new Path(linkedDir), true))
                .isEqualTo(true);
        assertThat(localFS.exists(new Path(testDir))).isFalse();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isFalse();
        assertThat(localFS.exists(new Path(linkedDir))).isFalse();
        assertThat(localFS.exists(new Path(src))).isFalse();
    }

    @TestTemplate
    void testRegisterFileBeforeRename() throws IOException {
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(
                        localFS,
                        createFileOwnershipDecider(),
                        tempDir.toString() + "/db",
                        tempDir.toString() + "/db");
        String testDir = tempDir + "/testDir";
        localFS.mkdirs(new Path(testDir));
        String src = testDir + "/source";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();
        registerFile(fileMappingManager, new Path(src));

        String linkedDirTmp = tempDir.toString() + "/linkedDir.tmp";
        localFS.mkdirs(new Path(linkedDirTmp));
        String linkedSrc = linkedDirTmp + "/source";

        // link src to linkedDirTmp
        fileMappingManager.link(src, linkedSrc);

        // create file in linkedDirTmp
        String create = linkedDirTmp + "/create.sst";
        MappingEntry createdMappingEntry = registerFile(fileMappingManager, new Path(create));
        FSDataOutputStream os1 =
                localFS.create(createdMappingEntry.getSourcePath(), FileSystem.WriteMode.OVERWRITE);
        os1.write(233);
        os1.close();

        String linkedDir = tempDir.toString() + "/linkedDir";
        // rename linkDir.tmp to linkedDir
        assertThat(fileMappingManager.renameFile(linkedDirTmp, linkedDir)).isEqualTo(true);
        linkedSrc = linkedDir + "/source";

        // delete src
        assertThat(fileMappingManager.deleteFileOrDirectory(new Path(src), false)).isEqualTo(true);
        assertThat(localFS.exists(new Path(testDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isTrue();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(src))).isTrue();

        // delete testDir
        fileMappingManager.deleteFileOrDirectory(new Path(testDir), true);
        assertThat(localFS.exists(new Path(testDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isTrue();
        assertThat(localFS.exists(new Path(src))).isTrue();

        // delete linkedSrc
        assertThat(fileMappingManager.deleteFileOrDirectory(new Path(linkedSrc), false))
                .isEqualTo(true);
        assertThat(localFS.exists(new Path(src))).isFalse();
        assertThat(localFS.exists(new Path(testDir))).isFalse();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isTrue();

        // delete create file
        String renamedCreated = linkedDir + "/create.sst";
        assertThat(fileMappingManager.deleteFileOrDirectory(new Path(renamedCreated), false))
                .isEqualTo(true);
        assertThat(localFS.exists(new Path(renamedCreated))).isFalse();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        if (reuseCp) {
            assertThat(localFS.exists(new Path(linkedDirTmp))).isTrue();
        } else {
            assertThat(localFS.exists(new Path(linkedDirTmp))).isFalse();
        }
        assertThat(localFS.exists(new Path(testDir))).isFalse();

        // delete linkedDir
        assertThat(fileMappingManager.deleteFileOrDirectory(new Path(linkedDir), true))
                .isEqualTo(true);
        assertThat(localFS.exists(new Path(testDir))).isFalse();
    }
}
