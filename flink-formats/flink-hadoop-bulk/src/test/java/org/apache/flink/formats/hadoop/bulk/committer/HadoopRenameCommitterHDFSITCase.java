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

package org.apache.flink.formats.hadoop.bulk.committer;

import org.apache.flink.formats.hadoop.bulk.AbstractFileCommitterTest;
import org.apache.flink.formats.hadoop.bulk.HadoopFileCommitter;
import org.apache.flink.formats.hadoop.bulk.committer.cluster.HDFSCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

/** Tests the behaviors of {@link HadoopRenameFileCommitter} with HDFS file system. */
@ExtendWith(MiniClusterExtension.class)
class HadoopRenameCommitterHDFSITCase extends AbstractFileCommitterTest {

    private static HDFSCluster hdfsCluster;

    @BeforeAll
    static void createHDFS(@TempDir File tmp) throws Exception {
        assumeFalse(OperatingSystem.isWindows());

        hdfsCluster = new HDFSCluster(tmp);
    }

    @AfterAll
    static void destroyHDFS() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
        }

        hdfsCluster = null;
    }

    @Override
    protected Path getBasePath() {
        return hdfsCluster.newFolder();
    }

    @Override
    protected Configuration getConfiguration() {
        return new Configuration();
    }

    @Override
    protected HadoopFileCommitter createNewCommitter(
            Configuration configuration, Path targetFilePath) throws IOException {

        return new HadoopRenameFileCommitter(configuration, targetFilePath);
    }

    @Override
    protected HadoopFileCommitter createPendingCommitter(
            Configuration configuration, Path targetFilePath, Path tempFilePath)
            throws IOException {

        return new HadoopRenameFileCommitter(configuration, targetFilePath, tempFilePath);
    }

    @Override
    protected void cleanup(Configuration configuration, Path basePath) throws IOException {
        basePath.getFileSystem(configuration).delete(basePath, true);
    }
}
