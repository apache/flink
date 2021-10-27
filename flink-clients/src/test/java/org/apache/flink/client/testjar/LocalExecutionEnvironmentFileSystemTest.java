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

package org.apache.flink.client.testjar;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.testutils.TestFileSystem;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;

class LocalExecutionEnvironmentFileSystemTest {
    @RegisterExtension
    static final Extension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            FileSystemFactory.class,
                            TestFileSystemFactoryWithConfiguration.class.getName())
                    .build();

    @TempDir Path tmp;

    private static final Map<String, String> EXPECTED_CONFIGURATION =
            ImmutableMap.of("s3.access-key", "user", "s3.secret-key", "secret");

    @Test
    void testConfigureFileSystem() throws Exception {
        final Configuration config = new Configuration();
        config.set(MiniCluster.PLUGIN_DIRECTORY, tmp.toAbsolutePath().toString());
        EXPECTED_CONFIGURATION.forEach(config::setString);
        final StreamExecutionEnvironment environment = new LocalStreamEnvironment(config);
        environment.getCheckpointConfig().setCheckpointStorage("testConfig:/" + tmp);
        environment.enableCheckpointing(100);
        environment.fromElements(1, 2, 3, 4).map((MapFunction<Integer, Integer>) value -> value);
        environment.execute("configureFilesystem");
    }

    /** Test filesystem to check that configurations are propagated. */
    public static final class TestFileSystemFactoryWithConfiguration implements FileSystemFactory {

        @Override
        public String getScheme() {
            return "testConfig";
        }

        @Override
        public FileSystem create(URI fsUri) throws IOException {
            return new TestFileSystem();
        }

        @Override
        public void configure(Configuration config) {
            EXPECTED_CONFIGURATION.forEach(
                    (k, v) ->
                            Assertions.assertEquals(
                                    v,
                                    config.getString(k, null),
                                    "Unexpected config entry for " + k));
        }
    }
}
