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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ActiveResourceManagerFactory}. */
public class ActiveResourceManagerFactoryTest extends TestLogger {

    private static final MemorySize TOTAL_FLINK_SIZE = MemorySize.ofMebiBytes(2 * 1024);
    private static final MemorySize TOTAL_PROCESS_SIZE = MemorySize.ofMebiBytes(3 * 1024);

    private static Map<String, String> systemEnv;

    @BeforeClass
    public static void setupClass() {
        systemEnv = System.getenv();
        System.clearProperty("flink.tests.disable-declarative");
        System.clearProperty("flink.tests.enable-fine-grained");
    }

    @AfterClass
    public static void teardownClass() {
        if (systemEnv != null) {
            CommonTestUtils.setEnv(systemEnv, true);
        }
    }

    @Test
    public void testGetEffectiveConfigurationForResourceManagerCoarseGrained() {
        final Configuration config = new Configuration();
        config.set(ClusterOptions.ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT, false);
        config.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_SIZE);
        config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, TOTAL_PROCESS_SIZE);

        final Configuration effectiveConfig =
                getFactory().getEffectiveConfigurationForResourceManager(config);

        assertTrue(effectiveConfig.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY));
        assertTrue(effectiveConfig.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
        assertThat(
                effectiveConfig.get(TaskManagerOptions.TOTAL_FLINK_MEMORY), is(TOTAL_FLINK_SIZE));
        assertThat(
                effectiveConfig.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY),
                is(TOTAL_PROCESS_SIZE));
    }

    @Test
    public void testGetEffectiveConfigurationForResourceManagerFineGrained() {
        final Configuration config = new Configuration();
        config.set(ClusterOptions.ENABLE_DECLARATIVE_RESOURCE_MANAGEMENT, true);
        config.set(ClusterOptions.ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT, true);
        config.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_SIZE);
        config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, TOTAL_PROCESS_SIZE);

        final Configuration effectiveConfig =
                getFactory().getEffectiveConfigurationForResourceManager(config);

        assertFalse(effectiveConfig.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY));
        assertFalse(effectiveConfig.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    private static ActiveResourceManagerFactory<ResourceID> getFactory() {
        return new ActiveResourceManagerFactory<ResourceID>() {
            @Override
            protected ResourceManagerRuntimeServicesConfiguration
                    createResourceManagerRuntimeServicesConfiguration(Configuration configuration)
                            throws ConfigurationException {
                return null;
            }

            @Override
            protected ResourceManagerDriver<ResourceID> createResourceManagerDriver(
                    Configuration configuration,
                    @Nullable String webInterfaceUrl,
                    String rpcAddress)
                    throws Exception {
                return null;
            }
        };
    }
}
