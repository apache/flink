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
import org.apache.flink.core.fs.FileSystemBehaviorTestSuite;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.VersionInfo;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assumptions.assumeThat;

/** Behavior tests for HDFS. */
class HadoopLocalFileSystemBehaviorTest extends FileSystemBehaviorTestSuite {

    @TempDir private java.nio.file.Path tmp;

    @Override
    protected FileSystem getFileSystem() throws Exception {
        org.apache.hadoop.fs.FileSystem fs = new RawLocalFileSystem();
        fs.initialize(LocalFileSystem.getLocalFsURI(), new Configuration());
        return new HadoopFileSystem(fs);
    }

    @Override
    protected Path getBasePath() throws Exception {
        return new Path(tmp.toUri());
    }

    @Override
    protected FileSystemKind getFileSystemKind() {
        return FileSystemKind.FILE_SYSTEM;
    }

    // ------------------------------------------------------------------------

    /** This test needs to be skipped for earlier Hadoop versions because those have a bug. */
    @Override
    protected void testMkdirsFailsForExistingFile() throws Exception {
        final String versionString = VersionInfo.getVersion();
        final String prefix = versionString.substring(0, 3);
        final float version = Float.parseFloat(prefix);
        assumeThat(version)
                .describedAs("Cannot execute this test on Hadoop prior to 2.8")
                .isGreaterThanOrEqualTo(2.8f);

        super.testMkdirsFailsForExistingFile();
    }
}
