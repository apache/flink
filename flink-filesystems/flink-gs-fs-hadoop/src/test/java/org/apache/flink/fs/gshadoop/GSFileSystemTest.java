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

package org.apache.flink.fs.gshadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** Tests the GS file system factory. */
public class GSFileSystemTest extends TestLogger {

    private static final URI FS_URI = URI.create("gs://bucket/name");

    private GSFileSystemFactory fileSystemFactory;

    @Before
    public void setup() {
        MockGSFileSystemHelper fileSystemHelper = new MockGSFileSystemHelper();
        fileSystemFactory = new GSFileSystemFactory(fileSystemHelper);
    }

    @Test
    public void testScheme() {
        assertEquals("gs", fileSystemFactory.getScheme());
    }

    @Test
    public void testCreateWithDefaultOptions() throws IOException {
        Configuration flinkConfig = new Configuration();
        fileSystemFactory.configure(flinkConfig);
        GSFileSystem fileSystem = (GSFileSystem) fileSystemFactory.create(FS_URI);

        assertNotNull(fileSystem);
        assertEquals(FS_URI, fileSystem.hadoopFileSystem.getUri());

        assertEquals(
                GSFileSystemFactory.DEFAULT_UPLOAD_CONTENT_TYPE,
                fileSystem.options.uploadContentType);
        assertNull(fileSystem.options.uploadTempBucket);
        assertEquals(
                GSFileSystemFactory.DEFAULT_UPLOAD_TEMP_PREFIX,
                fileSystem.options.uploadTempPrefix);
    }

    @Test
    public void testCreateWithNonDefaultOptions() throws IOException {
        final String specifiedBucket = "BUCKET";
        final String specifiedPrefix = "PREFIX/";
        final String specifiedContentTypE = "CONTENT_TYPE";

        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("gs.upload.temp.bucket", specifiedBucket);
        flinkConfig.setString("gs.upload.temp.prefix", specifiedPrefix);
        flinkConfig.setString("gs.upload.content.type", specifiedContentTypE);
        fileSystemFactory.configure(flinkConfig);
        GSFileSystem fileSystem = (GSFileSystem) fileSystemFactory.create(FS_URI);

        assertNotNull(fileSystem);
        assertNotNull(fileSystem.hadoopFileSystem);
        assertNotNull(fileSystem.options);
        assertEquals(FS_URI, fileSystem.hadoopFileSystem.getUri());

        assertEquals(specifiedBucket, fileSystem.options.uploadTempBucket);
        assertEquals(specifiedPrefix, fileSystem.options.uploadTempPrefix);
        assertEquals(specifiedContentTypE, fileSystem.options.uploadContentType);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateWithEmptyPrefix() throws IOException {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("gs.upload.temp.prefix", "");
        fileSystemFactory.configure(flinkConfig);
        fileSystemFactory.create(FS_URI);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateWithNullPrefix() throws IOException {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("gs.upload.temp.prefix", null);
        fileSystemFactory.configure(flinkConfig);
        fileSystemFactory.create(FS_URI);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithLeadingSeparatorPrefix() throws IOException {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("gs.upload.temp.prefix", "/prefix");
        fileSystemFactory.configure(flinkConfig);
        fileSystemFactory.create(FS_URI);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateWithEmptyContentType() throws IOException {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("gs.upload.content.type", "");
        fileSystemFactory.configure(flinkConfig);
        fileSystemFactory.create(FS_URI);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateWithNullContentType() throws IOException {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("gs.upload.content.type", null);
        fileSystemFactory.configure(flinkConfig);
        fileSystemFactory.create(FS_URI);
    }

    @Test
    public void testHadoopOptionsPassThrough() throws IOException {
        final String projectId = "PROJECT_ID";
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("fs.gs.project.id", projectId);
        fileSystemFactory.configure(flinkConfig);
        org.apache.flink.fs.gshadoop.GSFileSystem fileSystem =
                (org.apache.flink.fs.gshadoop.GSFileSystem) fileSystemFactory.create(FS_URI);
        assertEquals(projectId, fileSystem.hadoopFileSystem.getConf().get("fs.gs.project.id"));
    }
}
