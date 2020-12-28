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

package org.apache.flink.fs.openstackhadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Integration tests for the Swift file system support. */
public class HadoopSwiftFileSystemITCase extends TestLogger {

    private static final String SERVICENAME = "privatecloud";

    private static final String CONTAINER = System.getenv("ARTIFACTS_OS_CONTAINER");

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    private static final String AUTH_URL = System.getenv("ARTIFACTS_OS_AUTH_URL");
    private static final String USERNAME = System.getenv("ARTIFACTS_OS_USERNAME");
    private static final String PASSWORD = System.getenv("ARTIFACTS_OS_PASSWORD");
    private static final String TENANT = System.getenv("ARTIFACTS_OS_TENANT");
    private static final String REGION = System.getenv("ARTIFACTS_OS_REGION");

    /** Will be updated by {@link #checkCredentialsAndSetup()} if the test is not skipped. */
    private static boolean skipTest = true;

    @BeforeClass
    public static void checkCredentialsAndSetup() throws IOException {
        // check whether credentials exist
        Assume.assumeTrue("Swift container not configured, skipping test...", CONTAINER != null);
        Assume.assumeTrue("Swift username not configured, skipping test...", USERNAME != null);
        Assume.assumeTrue("Swift password not configured, skipping test...", PASSWORD != null);
        Assume.assumeTrue("Swift tenant not configured, skipping test...", TENANT != null);
        Assume.assumeTrue("Swift region not configured, skipping test...", REGION != null);

        // initialize configuration with valid credentials
        final Configuration conf = createConfiguration();

        FileSystem.initialize(conf);

        // check for uniqueness of the test directory
        final Path directory =
                new Path("swift://" + CONTAINER + '.' + SERVICENAME + '/' + TEST_DATA_DIR);
        final FileSystem fs = directory.getFileSystem();

        // directory must not yet exist
        assertFalse(fs.exists(directory));

        // reset configuration
        FileSystem.initialize(new Configuration());

        skipTest = false;
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        if (!skipTest) {
            // initialize configuration with valid credentials
            final Configuration conf = createConfiguration();
            FileSystem.initialize(conf);

            final Path directory =
                    new Path("swift://" + CONTAINER + '.' + SERVICENAME + '/' + TEST_DATA_DIR);
            final FileSystem fs = directory.getFileSystem();

            // clean up
            fs.delete(directory, true);

            // now directory must be gone
            assertFalse(fs.exists(directory));

            // reset configuration
            FileSystem.initialize(new Configuration());
        }
    }

    @Test
    public void testSimpleFileWriteAndRead() throws Exception {
        final Configuration conf = createConfiguration();

        final String testLine = "Hello Upload!";

        FileSystem.initialize(conf);

        final Path path =
                new Path(
                        "swift://"
                                + CONTAINER
                                + '.'
                                + SERVICENAME
                                + '/'
                                + TEST_DATA_DIR
                                + "/test.txt");
        final FileSystem fs = path.getFileSystem();

        try {
            try (FSDataOutputStream out = fs.create(path, WriteMode.OVERWRITE);
                    OutputStreamWriter writer =
                            new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                writer.write(testLine);
            }

            try (FSDataInputStream in = fs.open(path);
                    InputStreamReader ir = new InputStreamReader(in, StandardCharsets.UTF_8);
                    BufferedReader reader = new BufferedReader(ir)) {
                String line = reader.readLine();
                assertEquals(testLine, line);
            }
        } finally {
            fs.delete(path, false);
        }
    }

    @Test
    public void testDirectoryListing() throws Exception {
        final Configuration conf = createConfiguration();

        FileSystem.initialize(conf);

        final Path directory =
                new Path(
                        "swift://"
                                + CONTAINER
                                + '.'
                                + SERVICENAME
                                + '/'
                                + TEST_DATA_DIR
                                + "/testdir/");
        final FileSystem fs = directory.getFileSystem();

        // directory must not yet exist
        assertFalse(fs.exists(directory));

        try {
            // create directory
            assertTrue(fs.mkdirs(directory));

            // seems the file system does not assume existence of empty directories
            assertTrue(fs.exists(directory));

            // directory empty
            assertEquals(0, fs.listStatus(directory).length);

            // create some files
            final int numFiles = 3;
            for (int i = 0; i < numFiles; i++) {
                Path file = new Path(directory, "/file-" + i);
                try (FSDataOutputStream out = fs.create(file, FileSystem.WriteMode.NO_OVERWRITE);
                        OutputStreamWriter writer =
                                new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                    writer.write("hello-" + i + "\n");
                }
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
            fs.delete(directory, true);
        }

        // now directory must be gone
        assertFalse(fs.exists(directory));
    }

    private static Configuration createConfiguration() {
        final Configuration conf = new Configuration();
        conf.setString("swift.service." + SERVICENAME + ".auth.url", AUTH_URL);
        conf.setString("swift.service." + SERVICENAME + ".username", USERNAME);
        conf.setString("swift.service." + SERVICENAME + ".password", PASSWORD);
        conf.setString("swift.service." + SERVICENAME + ".tenant", TENANT);
        conf.setString("swift.service." + SERVICENAME + ".region", REGION);
        conf.setString("swift.service." + SERVICENAME + ".public", "true");
        return conf;
    }
}
