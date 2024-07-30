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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TestingTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TierFactoryInitializer}. */
public class TierFactoryInitializerTest {

    private static Path tmpDir;

    @BeforeAll
    public static void before(@TempDir Path path) throws Exception {
        tmpDir = TempDirUtils.newFolder(path, UUID.randomUUID().toString()).toPath();
    }

    @AfterAll
    public static void after() throws IOException {
        FileUtils.deleteDirectory(tmpDir.toFile());
    }

    @Test
    void testInitEphemeralTiers() {
        Configuration configuration = new Configuration();
        List<TierFactory> tierFactories =
                TierFactoryInitializer.initializeTierFactories(configuration);
        assertThat(tierFactories).hasSize(2);
        assertThat(tierFactories.get(0)).isInstanceOf(MemoryTierFactory.class);
        assertThat(tierFactories.get(1)).isInstanceOf(DiskTierFactory.class);
    }

    @Test
    void testInitEphemeralTiersWithRemoteTier() {
        Configuration configuration = new Configuration();
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH,
                tmpDir.toString());
        List<TierFactory> tierFactories =
                TierFactoryInitializer.initializeTierFactories(configuration);
        assertThat(tierFactories).hasSize(3);
        assertThat(tierFactories.get(0)).isInstanceOf(MemoryTierFactory.class);
        assertThat(tierFactories.get(1)).isInstanceOf(DiskTierFactory.class);
        assertThat(tierFactories.get(2)).isInstanceOf(RemoteTierFactory.class);
    }

    @Test
    void testInitDurableTiersWithExternalRemoteTier() {
        Configuration configuration = new Configuration();
        configuration.set(
                NettyShuffleEnvironmentOptions
                        .NETWORK_HYBRID_SHUFFLE_EXTERNAL_REMOTE_TIER_FACTORY_CLASS_NAME,
                ExternalRemoteTierFactory.class.getName());
        List<TierFactory> tierFactories =
                TierFactoryInitializer.initializeTierFactories(configuration);
        assertThat(tierFactories).hasSize(1);
        assertThat(tierFactories.get(0)).isInstanceOf(ExternalRemoteTierFactory.class);
    }

    @Test
    void testInitDurableExternalRemoteTierWithHigherPriority() {
        Configuration configuration = new Configuration();
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH,
                tmpDir.toString());
        configuration.set(
                NettyShuffleEnvironmentOptions
                        .NETWORK_HYBRID_SHUFFLE_EXTERNAL_REMOTE_TIER_FACTORY_CLASS_NAME,
                ExternalRemoteTierFactory.class.getName());
        List<TierFactory> tierFactories =
                TierFactoryInitializer.initializeTierFactories(configuration);
        assertThat(tierFactories).hasSize(1);
        assertThat(tierFactories.get(0)).isInstanceOf(ExternalRemoteTierFactory.class);
    }

    /** Testing implementation for {@link TierFactory} to init an external remote tier. */
    public static class ExternalRemoteTierFactory extends TestingTierFactory {
        @Override
        public void setup(Configuration configuration) {
            // noop
        }
    }
}
