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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.LimitedConnectionsFileSystem;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.apache.flink.configuration.ConfigurationUtils.getIntConfigOption;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test that the Hadoop file system wrapper correctly picks up connection limiting settings for the
 * correct file systems.
 */
class LimitedConnectionsConfigurationTest {

    @Test
    void testConfiguration() throws Exception {

        // nothing configured, we should get a regular file system
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:12345/a/b/c"));
        FileSystem ftpfs = FileSystem.get(URI.create("ftp://localhost:12345/a/b/c"));

        assertThat(hdfs).isNotInstanceOf(LimitedConnectionsFileSystem.class);
        assertThat(ftpfs).isNotInstanceOf(LimitedConnectionsFileSystem.class);

        // configure some limits, which should cause "fsScheme" to be limited

        final Configuration config = new Configuration();
        config.set(getIntConfigOption("fs.hdfs.limit.total"), 40);
        config.set(getIntConfigOption("fs.hdfs.limit.input"), 39);
        config.set(getIntConfigOption("fs.hdfs.limit.output"), 38);
        config.set(getIntConfigOption("fs.hdfs.limit.timeout"), 23456);
        config.set(getIntConfigOption("fs.hdfs.limit.stream-timeout"), 34567);

        try {
            FileSystem.initialize(config);

            hdfs = FileSystem.get(URI.create("hdfs://localhost:12345/a/b/c"));
            ftpfs = FileSystem.get(URI.create("ftp://localhost:12345/a/b/c"));

            assertThat(hdfs).isInstanceOf(LimitedConnectionsFileSystem.class);
            assertThat(ftpfs).isNotInstanceOf(LimitedConnectionsFileSystem.class);

            LimitedConnectionsFileSystem limitedFs = (LimitedConnectionsFileSystem) hdfs;
            assertThat(limitedFs.getMaxNumOpenStreamsTotal()).isEqualTo(40);
            assertThat(limitedFs.getMaxNumOpenInputStreams()).isEqualTo(39);
            assertThat(limitedFs.getMaxNumOpenOutputStreams()).isEqualTo(38);
            assertThat(limitedFs.getStreamOpenTimeout()).isEqualTo(23456);
            assertThat(limitedFs.getStreamInactivityTimeout()).isEqualTo(34567);
        } finally {
            // clear all settings
            FileSystem.initialize(new Configuration());
        }
    }
}
