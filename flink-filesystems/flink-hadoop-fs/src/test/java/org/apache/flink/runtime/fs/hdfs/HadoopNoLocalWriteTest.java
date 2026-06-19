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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class HadoopNoLocalWriteTest {

    @TempDir private static java.nio.file.Path tempFolder;

    private static MiniDFSCluster hdfsCluster;

    /** The cached file system instance. */
    private static FileSystem fileSystem;

    private static Path basePath;

    @BeforeAll
    static void testHadoopVersion() {
        assumeThat(HadoopUtils.isMinHadoopVersion(2, 6)).isTrue();
    }

    @BeforeAll
    static void verifyOS() {
        assumeThat(OperatingSystem.isWindows()).isFalse();
    }

    @BeforeAll
    static void createHDFS() throws Exception {
        final File baseDir = TempDirUtils.newFolder(tempFolder);

        final Configuration hdConf = new Configuration();
        hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        hdConf.set("dfs.replication", "2");
        hdConf.set("dfs.blocksize", String.valueOf(512));
        hdConf.set("dfs.namenode.fs-limits.min-block-size", String.valueOf(512));

        final MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf).numDataNodes(3);
        hdfsCluster = builder.build();

        final org.apache.hadoop.fs.FileSystem hdfs = hdfsCluster.getFileSystem();
        fileSystem = new HadoopFileSystem(hdfs);
        basePath = new Path(hdfs.getUri() + "/tests");
    }

    @Test
    void testNoLocalWriteFlag() throws Exception {
        createHDFS();
        final HadoopRecoverableWriter writer =
                (HadoopRecoverableWriter) getNoLocalWriteFileSystemWriter();
        BlockManager bm = hdfsCluster.getNameNode().getNamesystem().getBlockManager();
        DatanodeManager dm = bm.getDatanodeManager();
        try (RecoverableFsDataOutputStream os = writer.open(new Path("/tests/test-no-local"))) {
            // Inject a DatanodeManager that returns one DataNode as local node for
            // the client.
            DatanodeManager spyDm = spy(dm);
            DatanodeDescriptor dn1 =
                    dm.getDatanodeListForReport(HdfsConstants.DatanodeReportType.LIVE).get(0);
            doReturn(dn1).when(spyDm).getDatanodeByHost("127.0.0.1");
            Whitebox.setInternalState(bm, "datanodeManager", spyDm);
            byte[] buf = new byte[512 * 16];
            new Random().nextBytes(buf);
            os.write(buf);
        } finally {
            Whitebox.setInternalState(bm, "datanodeManager", dm);
        }
        hdfsCluster.triggerBlockReports();
        final String bpid = hdfsCluster.getNamesystem().getBlockPoolId();
        // Total number of DataNodes is 3.
        Assertions.assertEquals(3, hdfsCluster.getAllBlockReports(bpid).size());
        int numDataNodesWithData = 0;
        for (Map<DatanodeStorage, BlockListAsLongs> dnBlocks :
                hdfsCluster.getAllBlockReports(bpid)) {
            for (BlockListAsLongs blocks : dnBlocks.values()) {
                if (blocks.getNumberOfBlocks() > 0) {
                    numDataNodesWithData++;
                    break;
                }
            }
        }
        // Verify that only one DN has no data.
        Assertions.assertEquals(1, 3 - numDataNodesWithData);
    }

    @AfterAll
    static void destroyHDFS() throws Exception {
        if (hdfsCluster != null) {
            hdfsCluster
                    .getFileSystem()
                    .delete(new org.apache.hadoop.fs.Path(basePath.toUri()), true);
            hdfsCluster.shutdown();
        }
    }

    private RecoverableWriter getNoLocalWriteFileSystemWriter() throws Exception {
        Map<String, String> conf = new HashMap<>();
        conf.put("fs.hdfs.no-local-write", "true");
        return fileSystem.createRecoverableWriter(conf);
    }
}
