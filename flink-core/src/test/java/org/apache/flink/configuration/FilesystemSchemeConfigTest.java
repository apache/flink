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

package org.apache.flink.configuration;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.UnsupportedFileSystemSchemeException;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the configuration of the default file system scheme. */
public class FilesystemSchemeConfigTest extends TestLogger {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    @After
    public void clearFsSettings() throws IOException {
        FileSystem.initialize(new Configuration());
    }

    // ------------------------------------------------------------------------

    @Test
    public void testDefaultsToLocal() throws Exception {
        URI justPath = new URI(tempFolder.newFile().toURI().getPath());
        assertNull(justPath.getScheme());

        FileSystem fs = FileSystem.get(justPath);
        assertEquals("file", fs.getUri().getScheme());
    }

    @Test
    public void testExplicitlySetToLocal() throws Exception {
        final Configuration conf = new Configuration();
        conf.setString(
                CoreOptions.DEFAULT_FILESYSTEM_SCHEME, LocalFileSystem.getLocalFsURI().toString());
        FileSystem.initialize(conf);

        URI justPath = new URI(tempFolder.newFile().toURI().getPath());
        assertNull(justPath.getScheme());

        FileSystem fs = FileSystem.get(justPath);
        assertEquals("file", fs.getUri().getScheme());
    }

    @Test
    public void testExplicitlySetToOther() throws Exception {
        final Configuration conf = new Configuration();
        conf.setString(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "otherFS://localhost:1234/");
        FileSystem.initialize(conf);

        URI justPath = new URI(tempFolder.newFile().toURI().getPath());
        assertNull(justPath.getScheme());

        try {
            FileSystem.get(justPath);
            fail("should have failed with an exception");
        } catch (UnsupportedFileSystemSchemeException e) {
            assertTrue(e.getMessage().contains("otherFS"));
        }
    }

    @Test
    public void testExplicitlyPathTakesPrecedence() throws Exception {
        final Configuration conf = new Configuration();
        conf.setString(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "otherFS://localhost:1234/");
        FileSystem.initialize(conf);

        URI pathAndScheme = tempFolder.newFile().toURI();
        assertNotNull(pathAndScheme.getScheme());

        FileSystem fs = FileSystem.get(pathAndScheme);
        assertEquals("file", fs.getUri().getScheme());
    }
}
