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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the configuration of the default file system scheme. */
class FilesystemSchemeConfigTest {

    @TempDir private File tempFolder;

    @AfterEach
    void clearFsSettings() {
        FileSystem.initialize(new Configuration());
    }

    // ------------------------------------------------------------------------

    @Test
    void testDefaultsToLocal() throws Exception {
        URI justPath = new URI(File.createTempFile("junit", null, tempFolder).toURI().getPath());
        assertThat(justPath.getScheme()).isNull();

        FileSystem fs = FileSystem.get(justPath);
        assertThat(fs.getUri().getScheme()).isEqualTo("file");
    }

    @Test
    void testExplicitlySetToLocal() throws Exception {
        final Configuration conf = new Configuration();
        conf.set(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, LocalFileSystem.getLocalFsURI().toString());
        FileSystem.initialize(conf);

        URI justPath = new URI(File.createTempFile("junit", null, tempFolder).toURI().getPath());
        assertThat(justPath.getScheme()).isNull();

        FileSystem fs = FileSystem.get(justPath);
        assertThat(fs.getUri().getScheme()).isEqualTo("file");
    }

    @Test
    void testExplicitlySetToOther() throws Exception {
        final Configuration conf = new Configuration();
        conf.set(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "otherFS://localhost:1234/");
        FileSystem.initialize(conf);

        URI justPath = new URI(File.createTempFile("junit", null, tempFolder).toURI().getPath());
        assertThat(justPath.getScheme()).isNull();

        assertThatThrownBy(() -> FileSystem.get(justPath))
                .isInstanceOf(UnsupportedFileSystemSchemeException.class)
                .hasMessageContaining("otherFS");
    }

    @Test
    void testExplicitlyPathTakesPrecedence() throws Exception {
        final Configuration conf = new Configuration();
        conf.set(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "otherFS://localhost:1234/");
        FileSystem.initialize(conf);

        URI pathAndScheme = File.createTempFile("junit", null, tempFolder).toURI();
        assertThat(pathAndScheme.getScheme()).isNotNull();

        FileSystem fs = FileSystem.get(pathAndScheme);
        assertThat(fs.getUri().getScheme()).isEqualTo("file");
    }
}
