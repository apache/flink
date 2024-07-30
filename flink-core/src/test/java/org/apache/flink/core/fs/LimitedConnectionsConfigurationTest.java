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

package org.apache.flink.core.fs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.LimitedConnectionsFileSystem.ConnectionLimitingSettings;
import org.apache.flink.testutils.TestFileSystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that validate that the configuration for limited FS connections are properly picked up. */
class LimitedConnectionsConfigurationTest {

    @TempDir public File tempDir;

    /**
     * This test validates that the File System is correctly wrapped by the file system factories
     * when the corresponding entries are in the configuration.
     */
    @Test
    void testConfiguration() throws Exception {
        final String fsScheme = TestFileSystem.SCHEME;

        // nothing configured, we should get a regular file system
        FileSystem schemeFs = FileSystem.get(URI.create(fsScheme + ":///a/b/c"));
        FileSystem localFs = FileSystem.get(File.createTempFile("junit", null, tempDir).toURI());

        assertThat(schemeFs).isNotInstanceOf(LimitedConnectionsFileSystem.class);
        assertThat(localFs).isNotInstanceOf(LimitedConnectionsFileSystem.class);

        // configure some limits, which should cause "fsScheme" to be limited

        final Configuration config = new Configuration();
        config.setInteger("fs." + fsScheme + ".limit.total", 42);
        config.setInteger("fs." + fsScheme + ".limit.input", 11);
        config.setInteger("fs." + fsScheme + ".limit.output", 40);
        config.setInteger("fs." + fsScheme + ".limit.timeout", 12345);
        config.setInteger("fs." + fsScheme + ".limit.stream-timeout", 98765);

        try {
            FileSystem.initialize(config);

            schemeFs = FileSystem.get(URI.create(fsScheme + ":///a/b/c"));
            localFs = FileSystem.get(File.createTempFile("junit", null, tempDir).toURI());

            assertThat(schemeFs).isInstanceOf(LimitedConnectionsFileSystem.class);
            assertThat(localFs).isNotInstanceOf(LimitedConnectionsFileSystem.class);

            LimitedConnectionsFileSystem limitedFs = (LimitedConnectionsFileSystem) schemeFs;
            assertThat(limitedFs.getMaxNumOpenStreamsTotal()).isEqualTo(42);
            assertThat(limitedFs.getMaxNumOpenInputStreams()).isEqualTo(11);
            assertThat(limitedFs.getMaxNumOpenOutputStreams()).isEqualTo(40);
            assertThat(limitedFs.getStreamOpenTimeout()).isEqualTo(12345);
            assertThat(limitedFs.getStreamInactivityTimeout()).isEqualTo(98765);
        } finally {
            // clear all settings
            FileSystem.initialize(new Configuration());
        }
    }

    /**
     * This test checks that the file system connection limiting configuration object is properly
     * created.
     */
    @Test
    void testConnectionLimitingSettings() {
        final String scheme = "testscheme";

        // empty config
        assertThat(ConnectionLimitingSettings.fromConfig(new Configuration(), scheme)).isNull();

        // only total limit set
        {
            Configuration conf = new Configuration();
            conf.set(CoreOptions.fileSystemConnectionLimit(scheme), 10);

            ConnectionLimitingSettings settings =
                    ConnectionLimitingSettings.fromConfig(conf, scheme);
            assertThat(settings).isNotNull();
            assertThat(settings.limitTotal).isEqualTo(10);
            assertThat(settings.limitInput).isZero();
            assertThat(settings.limitOutput).isZero();
        }

        // only input limit set
        {
            Configuration conf = new Configuration();
            conf.set(CoreOptions.fileSystemConnectionLimitIn(scheme), 10);

            ConnectionLimitingSettings settings =
                    ConnectionLimitingSettings.fromConfig(conf, scheme);
            assertThat(settings).isNotNull();
            assertThat(settings.limitTotal).isZero();
            assertThat(settings.limitInput).isEqualTo(10);
            assertThat(settings.limitOutput).isZero();
        }

        // only output limit set
        {
            Configuration conf = new Configuration();
            conf.set(CoreOptions.fileSystemConnectionLimitOut(scheme), 10);

            ConnectionLimitingSettings settings =
                    ConnectionLimitingSettings.fromConfig(conf, scheme);
            assertThat(settings).isNotNull();
            assertThat(settings.limitTotal).isZero();
            assertThat(settings.limitInput).isZero();
            assertThat(settings.limitOutput).isEqualTo(10);
        }
    }
}
