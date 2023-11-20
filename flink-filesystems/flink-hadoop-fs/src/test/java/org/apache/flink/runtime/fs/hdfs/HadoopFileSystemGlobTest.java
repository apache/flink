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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Test for {@link org.apache.hadoop.fs.FileSystem} globStatus API support. */
public class HadoopFileSystemGlobTest {

    @ClassRule public static final TemporaryFolder TMP = new TemporaryFolder();
    private static final String TEST_FILE = "test-file-1";
    private static MiniDFSCluster hdfsCluster;
    private static org.apache.flink.core.fs.FileSystem fs;
    private static Path basePath;

    // ------------------------------------------------------------------------

    @BeforeClass
    public static void verifyOS() {
        Assume.assumeTrue(
                "HDFS cluster cannot be started on Windows without extensions.",
                !OperatingSystem.isWindows());
    }

    @BeforeClass
    public static void createHDFS() throws Exception {
        final File baseDir = TMP.newFolder();

        Configuration hdConf = new Configuration();
        hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
        hdfsCluster = builder.build();

        org.apache.hadoop.fs.FileSystem hdfs = hdfsCluster.getFileSystem();
        fs = new HadoopFileSystem(hdfs);

        basePath = new Path(hdfs.getUri().toString() + "/tests");
    }

    @AfterClass
    public static void destroyHDFS() throws Exception {
        if (hdfsCluster != null) {
            hdfsCluster
                    .getFileSystem()
                    .delete(new org.apache.hadoop.fs.Path(basePath.toUri()), true);
            hdfsCluster.shutdown();
        }
    }

    // ------------------------------------------------------------------------

    @Before
    public void setUp() throws Exception {
        fs.mkdirs(basePath);
        try (FSDataOutputStream os =
                fs.create(basePath.suffix("/" + TEST_FILE), WriteMode.OVERWRITE)) {
            os.write("DummyDataForA".getBytes(StandardCharsets.UTF_8));
            os.flush();
        }
    }

    @Test
    public void testGlobStatusSupported() {
        assertTrue(fs.isGlobStatusSupported());
    }

    @Test
    public void testGlobStatusWhenMatchingFilesFound() throws IOException {
        final Path globPattern = basePath.suffix("/*");
        final Path expectedFilePath = basePath.suffix("/" + TEST_FILE);

        FileStatus[] fileStatusResult = fs.globStatus(globPattern);
        assertEquals(1, fileStatusResult.length);
        assertEquals(expectedFilePath, fileStatusResult[0].getPath());
    }

    @Test
    public void testGlobStatusWhenNoMatchingFilesFound() throws IOException {
        FileStatus[] fileStatusResult1 = fs.globStatus(basePath.suffix("/invalid_file_name"));
        assertNull(fileStatusResult1);

        FileStatus[] fileStatusResult2 = fs.globStatus(basePath.suffix("/invalid_dir/*"));
        assertEquals(fileStatusResult2.length, 0);
    }

    @Test(expected = IOException.class)
    public void testGlobStatusThrowsIOExceptionWhenInvalidInput() throws IOException {
        Path invalidGlobPattern = basePath.suffix("[unclosed_parenthesis");
        fs.globStatus(invalidGlobPattern);
    }
}
