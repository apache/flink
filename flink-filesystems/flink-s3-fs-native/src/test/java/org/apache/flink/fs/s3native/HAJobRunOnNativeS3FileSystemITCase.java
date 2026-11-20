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

package org.apache.flink.fs.s3native;

import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collection;

/** Runs the HA job test with the default HTTP client and with the CRT client. */
@ExtendWith(ParameterizedTestExtension.class)
class HAJobRunOnNativeS3FileSystemITCase extends AbstractNativeS3HAJobRunITCase {

    @RegisterExtension
    private static final AllCallbackWrapper<TestContainerExtension<SeaweedFsNativeS3TestContainer>>
            SEAWEEDFS_EXTENSION =
                    new AllCallbackWrapper<>(
                            new TestContainerExtension<>(SeaweedFsNativeS3TestContainer::new));

    @Parameter private boolean crtEnabled;

    private MiniClusterResource cluster;

    @Parameters(name = "crt={0}")
    private static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @BeforeEach
    void startCluster() throws Exception {
        cluster =
                new MiniClusterResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(
                                        createConfiguration(
                                                getSeaweedFsContainer(),
                                                getClusterId(),
                                                crtEnabled))
                                .build());
        cluster.before();
    }

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.after();
        }
    }

    // Overridden without @Test so JUnit skips it; startCluster() starts a cluster per parameter.
    @Override
    public void testJobExecutionInHaMode(MiniCluster flinkCluster) {}

    @TestTemplate
    void testJobExecutionInHaModePerHttpClient() throws Exception {
        runJobInHaMode(cluster.getMiniCluster());
    }

    @Override
    SeaweedFsNativeS3TestContainer getSeaweedFsContainer() {
        return SEAWEEDFS_EXTENSION.getCustomExtension().getTestContainer();
    }

    @Override
    String getClusterId() {
        return crtEnabled ? "test-cluster-crt" : "test-cluster";
    }
}
