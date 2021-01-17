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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.core.fs.FileSystemTestUtils.checkPathEventualExistence;

/** Abstract integration test class for implementations of hadoop file system. */
public abstract class AbstractHadoopFileSystemITTest extends TestLogger {

    protected static FileSystem fs;
    protected static Path basePath;
    protected static long consistencyToleranceNS;

    public static void checkPathExistence(
            Path path, boolean expectedExists, long consistencyToleranceNS)
            throws IOException, InterruptedException {
        if (consistencyToleranceNS == 0) {
            // strongly consistency
            assertEquals(expectedExists, fs.exists(path));
        } else {
            // eventually consistency
            checkPathEventualExistence(fs, path, expectedExists, consistencyToleranceNS);
        }
    }

    protected void checkEmptyDirectory(Path path) throws IOException, InterruptedException {
        checkPathExistence(path, true, consistencyToleranceNS);
    }

    @Test
    public void testSimpleFileWriteAndRead() throws Exception {
        final String testLine = "Hello Upload!";

        final Path path = new Path(basePath, "test.txt");

        try {
            try (FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.OVERWRITE);
                    OutputStreamWriter writer =
                            new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                writer.write(testLine);
            }

            // just in case, wait for the path to exist
            checkPathExistence(path, true, consistencyToleranceNS);

            try (FSDataInputStream in = fs.open(path);
                    InputStreamReader ir = new InputStreamReader(in, StandardCharsets.UTF_8);
                    BufferedReader reader = new BufferedReader(ir)) {
                String line = reader.readLine();
                assertEquals(testLine, line);
            }
        } finally {
            fs.delete(path, false);
        }

        checkPathExistence(path, false, consistencyToleranceNS);
    }

    @Test
    public void testDirectoryListing() throws Exception {
        final Path directory = new Path(basePath, "testdir/");

        // directory must not yet exist
        assertFalse(fs.exists(directory));

        try {
            // create directory
            assertTrue(fs.mkdirs(directory));

            checkEmptyDirectory(directory);

            // directory empty
            assertEquals(0, fs.listStatus(directory).length);

            // create some files
            final int numFiles = 3;
            for (int i = 0; i < numFiles; i++) {
                Path file = new Path(directory, "/file-" + i);
                try (FSDataOutputStream out = fs.create(file, FileSystem.WriteMode.OVERWRITE);
                        OutputStreamWriter writer =
                                new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                    writer.write("hello-" + i + "\n");
                }
                // just in case, wait for the file to exist (should then also be reflected in the
                // directory's file list below)
                checkPathExistence(file, true, consistencyToleranceNS);
            }

            FileStatus[] files = fs.listStatus(directory);
            assertNotNull(files);
            assertEquals(3, files.length);

            for (FileStatus status : files) {
                assertFalse(status.isDir());
            }

            // now that there are files, the directory must exist
            assertTrue(fs.exists(directory));
        } finally {
            // clean up
            cleanupDirectoryWithRetry(fs, directory, consistencyToleranceNS);
        }
    }

    @AfterClass
    public static void teardown() throws IOException, InterruptedException {
        try {
            if (fs != null) {
                cleanupDirectoryWithRetry(fs, basePath, consistencyToleranceNS);
            }
        } finally {
            FileSystem.initialize(new Configuration());
        }
    }

    public static void cleanupDirectoryWithRetry(
            FileSystem fs, Path path, long consistencyToleranceNS)
            throws IOException, InterruptedException {
        fs.delete(path, true);
        long deadline = System.nanoTime() + consistencyToleranceNS;
        while (fs.exists(path) && System.nanoTime() - deadline < 0) {
            fs.delete(path, true);
            Thread.sleep(50L);
        }
        Assert.assertFalse(fs.exists(path));
    }
}
