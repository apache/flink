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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.state.forst.fs.filemapping.FileMappingManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link FileMappingManager}. */
public class FileMappingManagerTest {
    @TempDir static java.nio.file.Path tempDir;

    @Test
    void testFileLink() throws IOException {
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(localFS, localFS, tempDir.toString(), tempDir.toString());
        String src = tempDir + "/source";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();
        String dst = tempDir.toString() + "/dst";
        fileMappingManager.link(src, dst);
        assertThat(fileMappingManager.realPath(new Path(dst)).path.toString()).isEqualTo(src);
    }

    @Test
    void testNestLink() throws IOException {
        // link b->a
        // link c->b
        // link d->c
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(localFS, localFS, tempDir.toString(), tempDir.toString());
        String src = tempDir + "/a";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();
        String dstB = tempDir.toString() + "/b";
        fileMappingManager.link(src, dstB);
        assertThat(fileMappingManager.realPath(new Path(dstB)).path.toString()).isEqualTo(src);
        assertThat(fileMappingManager.mappingEntry(dstB).getReferenceCount()).isEqualTo(2);

        String dstC = tempDir.toString() + "/c";
        fileMappingManager.link(dstB, dstC);
        assertThat(fileMappingManager.realPath(new Path(dstC)).path.toString()).isEqualTo(src);
        assertThat(fileMappingManager.mappingEntry(dstC).getReferenceCount()).isEqualTo(3);

        String dstD = tempDir.toString() + "/d";
        fileMappingManager.link(dstC, dstD);
        assertThat(fileMappingManager.realPath(new Path(dstD)).path.toString()).isEqualTo(src);
        assertThat(fileMappingManager.mappingEntry(dstC).getReferenceCount()).isEqualTo(4);

        assertThat(fileMappingManager.link(dstD, dstC)).isEqualTo(-1);
    }

    @Test
    void testFileDelete() throws IOException {
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(localFS, localFS, tempDir.toString(), tempDir.toString());
        String src = tempDir + "/source";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();
        String dst = tempDir.toString() + "/dst";
        fileMappingManager.link(src, dst);
        // delete src
        fileMappingManager.deleteFile(new Path(src), false);
        assertThat(localFS.exists(new Path(src))).isTrue();

        // delete dst
        fileMappingManager.deleteFile(new Path(dst), false);
        assertThat(localFS.exists(new Path(src))).isFalse();
    }

    @Test
    void testDirectoryDelete() throws IOException {
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(localFS, localFS, tempDir.toString(), tempDir.toString());
        String testDir = tempDir + "/testDir";
        localFS.mkdirs(new Path(testDir));
        String src = testDir + "/source";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();
        String dst = tempDir.toString() + "/dst";
        fileMappingManager.link(src, dst);

        // delete testDir
        fileMappingManager.deleteFile(new Path(testDir), true);
        assertThat(localFS.exists(new Path(src))).isTrue();
        assertThat(localFS.exists(new Path(testDir))).isTrue();

        // delete dst
        fileMappingManager.deleteFile(new Path(dst), false);
        assertThat(localFS.exists(new Path(src))).isFalse();
        assertThat(localFS.exists(new Path(testDir))).isFalse();
    }

    @Test
    void testDirectoryRename() throws IOException {
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(localFS, localFS, tempDir.toString(), tempDir.toString());
        String testDir = tempDir + "/testDir";
        localFS.mkdirs(new Path(testDir));
        String src = testDir + "/source";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();

        String linkedDirTmp = tempDir.toString() + "/linkedDir.tmp";
        localFS.mkdirs(new Path(linkedDirTmp));
        String linkedSrc = linkedDirTmp + "/source";
        fileMappingManager.link(src, linkedSrc);

        String linkedDir = tempDir.toString() + "/linkedDir";
        // rename linkDir.tmp to linkedDir
        assertThat(fileMappingManager.renameFile(linkedDirTmp, linkedDir)).isEqualTo(true);
        linkedSrc = linkedDir + "/source";

        // delete src
        assertThat(fileMappingManager.deleteFile(new Path(src), false)).isEqualTo(true);
        assertThat(localFS.exists(new Path(testDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isFalse();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(src))).isTrue();

        // delete testDir
        fileMappingManager.deleteFile(new Path(testDir), true);
        assertThat(localFS.exists(new Path(testDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(src))).isTrue();

        // delete linkedSrc
        assertThat(fileMappingManager.deleteFile(new Path(linkedSrc), false)).isEqualTo(true);
        assertThat(localFS.exists(new Path(src))).isFalse();
        assertThat(localFS.exists(new Path(testDir))).isFalse();

        // delete linkedDir
        assertThat(fileMappingManager.deleteFile(new Path(linkedDir), true)).isEqualTo(true);
        assertThat(localFS.exists(new Path(testDir))).isFalse();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isFalse();
        assertThat(localFS.exists(new Path(linkedDir))).isFalse();
        assertThat(localFS.exists(new Path(src))).isFalse();
    }

    @Test
    void testCreateFileBeforeRename() throws IOException {
        FileSystem localFS = FileSystem.getLocalFileSystem();
        FileMappingManager fileMappingManager =
                new FileMappingManager(localFS, localFS, tempDir.toString(), tempDir.toString());
        String testDir = tempDir + "/testDir";
        localFS.mkdirs(new Path(testDir));
        String src = testDir + "/source";
        FSDataOutputStream os = localFS.create(new Path(src), FileSystem.WriteMode.OVERWRITE);
        os.write(233);
        os.close();

        String linkedDirTmp = tempDir.toString() + "/linkedDir.tmp";
        localFS.mkdirs(new Path(linkedDirTmp));
        String linkedSrc = linkedDirTmp + "/source";

        // link src to linkedDirTmp
        fileMappingManager.link(src, linkedSrc);

        // create file in linkedDirTmp
        String create = linkedDirTmp + "/create.sst";
        FileMappingManager.RealPath realPath = fileMappingManager.createFile(new Path(create));
        FSDataOutputStream os1 = localFS.create(realPath.path, FileSystem.WriteMode.OVERWRITE);
        os1.write(233);
        os1.close();

        String linkedDir = tempDir.toString() + "/linkedDir";
        // rename linkDir.tmp to linkedDir
        assertThat(fileMappingManager.renameFile(linkedDirTmp, linkedDir)).isEqualTo(true);
        linkedSrc = linkedDir + "/source";

        // delete src
        assertThat(fileMappingManager.deleteFile(new Path(src), false)).isEqualTo(true);
        assertThat(localFS.exists(new Path(testDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isTrue();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(src))).isTrue();

        // delete testDir
        fileMappingManager.deleteFile(new Path(testDir), true);
        assertThat(localFS.exists(new Path(testDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isTrue();
        assertThat(localFS.exists(new Path(src))).isTrue();

        // delete linkedSrc
        assertThat(fileMappingManager.deleteFile(new Path(linkedSrc), false)).isEqualTo(true);
        assertThat(localFS.exists(new Path(src))).isFalse();
        assertThat(localFS.exists(new Path(testDir))).isFalse();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isTrue();

        // delete create file
        String renamedCreated = linkedDir + "/create.sst";
        assertThat(fileMappingManager.deleteFile(new Path(renamedCreated), false)).isEqualTo(true);
        assertThat(localFS.exists(new Path(renamedCreated))).isFalse();
        assertThat(localFS.exists(new Path(linkedDir))).isTrue();
        assertThat(localFS.exists(new Path(linkedDirTmp))).isFalse();
        assertThat(localFS.exists(new Path(testDir))).isFalse();

        // delete linkedDir
        assertThat(fileMappingManager.deleteFile(new Path(linkedDir), true)).isEqualTo(true);
        assertThat(localFS.exists(new Path(testDir))).isFalse();
    }
}
