/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.rpc.RpcSystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the {@link PekkoRpcSystemLoader}.
 *
 * <p>This must be an ITCase so that it runs after the 'package' phase of maven. Otherwise the
 * flink-rpc-akka jar will not be available.
 */
class PekkoRpcSystemLoaderITCase {

    private static final PekkoRpcSystemLoader LOADER = new PekkoRpcSystemLoader();

    @Test
    void testServiceLoadingWithDefaultConfig() {
        final Configuration config = new Configuration();
        try (final RpcSystem rpcSystem = LOADER.loadRpcSystem(config)) {
            assertThat(rpcSystem).isNotNull();
        }
    }

    @Test
    void testServiceLoadingWithNonExistingPath(@TempDir Path tempDir) {
        final Configuration config = new Configuration();
        config.set(
                CoreOptions.TMP_DIRS, tempDir.resolve(Paths.get("some", "directory")).toString());
        try (final RpcSystem rpcSystem = LOADER.loadRpcSystem(config)) {
            assertThat(rpcSystem).isNotNull();
        }
    }

    @Test
    void testServiceLoadingWithExistingLinkedPath(@TempDir Path tempDir) throws Exception {
        final Configuration config = new Configuration();

        Path linkedDirectory = Paths.get(tempDir.toString(), "linkedDir");
        Path symbolicLink = Paths.get(tempDir.toString(), "symlink");
        Files.createSymbolicLink(symbolicLink, linkedDirectory);
        Files.createDirectories(linkedDirectory.resolve("a").resolve("b"));
        // set the tmp dirs to dirs in symbolic link path.
        config.set(CoreOptions.TMP_DIRS, symbolicLink.resolve("a").resolve("b").toString());
        try (final RpcSystem rpcSystem = LOADER.loadRpcSystem(config)) {
            assertThat(rpcSystem).isNotNull();
        }
    }

    @Test
    void testServiceLoadingWithNonExistingLinkedPath(@TempDir Path tempDir) throws Exception {
        final Configuration config = new Configuration();

        Path linkedDirectory = Paths.get(tempDir.toString(), "linkedDir");
        Path symbolicLink = Paths.get(tempDir.toString(), "symlink");
        Files.createSymbolicLink(symbolicLink, linkedDirectory);
        // set the tmp dirs to dirs in symbolic link path.
        config.set(CoreOptions.TMP_DIRS, symbolicLink.toString());
        // if this is a symlink that linked dir not exist, throw exception directly.
        assertThatThrownBy(() -> LOADER.loadRpcSystem(config))
                .hasRootCauseInstanceOf(NoSuchFileException.class);
    }
}
