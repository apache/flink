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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link HighAvailabilityServicesUtils} class. */
class HighAvailabilityServicesUtilsTest {

    @Test
    void testCreateCustomHAServices() throws Exception {
        Configuration config = new Configuration();

        HighAvailabilityServices haServices = new TestingHighAvailabilityServices();
        TestHAFactory.haServices = haServices;

        Executor executor = Executors.directExecutor();

        config.set(HighAvailabilityOptions.HA_MODE, TestHAFactory.class.getName());

        // when
        HighAvailabilityServices actualHaServices =
                HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
                        config, executor, NoOpFatalErrorHandler.INSTANCE);

        // then
        assertThat(actualHaServices).isSameAs(haServices);

        // when
        actualHaServices =
                HighAvailabilityServicesUtils.createHighAvailabilityServices(
                        config,
                        executor,
                        AddressResolution.NO_ADDRESS_RESOLUTION,
                        RpcSystem.load(),
                        NoOpFatalErrorHandler.INSTANCE);

        // then
        assertThat(actualHaServices).isSameAs(haServices);
    }

    @Test
    void testCreateCustomClientHAServices() throws Exception {
        Configuration config = new Configuration();

        ClientHighAvailabilityServices clientHAServices =
                TestingClientHAServices.createClientHAServices();
        TestHAFactory.clientHAServices = clientHAServices;

        config.set(HighAvailabilityOptions.HA_MODE, TestHAFactory.class.getName());

        // when
        ClientHighAvailabilityServices actualClientHAServices =
                HighAvailabilityServicesUtils.createClientHAService(
                        config, NoOpFatalErrorHandler.INSTANCE);

        // then
        assertThat(actualClientHAServices).isSameAs(clientHAServices);
    }

    @Test
    void testCustomHAServicesFactoryNotDefined() {
        Configuration config = new Configuration();

        Executor executor = Executors.directExecutor();

        config.set(
                HighAvailabilityOptions.HA_MODE,
                HighAvailabilityMode.FACTORY_CLASS.name().toLowerCase());

        assertThatThrownBy(
                        () ->
                                HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
                                        config, executor, NoOpFatalErrorHandler.INSTANCE))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testGetClusterHighAvailableStoragePath(@TempDir File tempDir) {
        final String haStorageRootDirectory = tempDir.getAbsolutePath();
        final String clusterId = UUID.randomUUID().toString();
        final Configuration configuration = new Configuration();

        configuration.set(HighAvailabilityOptions.HA_STORAGE_PATH, haStorageRootDirectory);
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);

        final Path clusterHighAvailableStoragePath =
                HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration);

        final Path expectedPath = new Path(haStorageRootDirectory, clusterId);
        assertThat(clusterHighAvailableStoragePath).isEqualTo(expectedPath);
    }

    /** Testing class which needs to be public in order to be instantiatable. */
    public static class TestHAFactory implements HighAvailabilityServicesFactory {

        static HighAvailabilityServices haServices;
        static ClientHighAvailabilityServices clientHAServices;

        @Override
        public HighAvailabilityServices createHAServices(
                Configuration configuration, Executor executor) {
            return haServices;
        }

        @Override
        public ClientHighAvailabilityServices createClientHAServices(Configuration configuration)
                throws Exception {
            return clientHAServices;
        }
    }
}
