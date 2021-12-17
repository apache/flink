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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.runtime.blob.VoidBlobStore;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Tests for the {@link KubernetesHaServices}. */
public class KubernetesHaServicesTest extends KubernetesHighAvailabilityTestBase {

    @Test
    public void testInternalCloseShouldCloseFlinkKubeClient() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final KubernetesHaServices kubernetesHaServices =
                                    new KubernetesHaServices(
                                            flinkKubeClient,
                                            executorService,
                                            configuration,
                                            new VoidBlobStore());
                            kubernetesHaServices.internalClose();
                            assertThat(closeKubeClientFuture.isDone(), is(true));
                        });
            }
        };
    }

    @Test
    public void testInternalCleanupShouldCleanupConfigMaps() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final KubernetesHaServices kubernetesHaServices =
                                    new KubernetesHaServices(
                                            flinkKubeClient,
                                            executorService,
                                            configuration,
                                            new VoidBlobStore());
                            kubernetesHaServices.internalCleanup();
                            final Map<String, String> labels =
                                    deleteConfigMapByLabelsFuture.get(
                                            TIMEOUT, TimeUnit.MILLISECONDS);
                            assertThat(labels.size(), is(3));
                            assertThat(
                                    labels.get(LABEL_CONFIGMAP_TYPE_KEY),
                                    is(LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
                        });
            }
        };
    }

    @Test
    public void testInternalJobCleanupShouldCleanupConfigMaps() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final KubernetesHaServices kubernetesHaServices =
                                    new KubernetesHaServices(
                                            flinkKubeClient,
                                            executorService,
                                            configuration,
                                            new VoidBlobStore());
                            JobID jobID = new JobID();
                            String configMapName =
                                    kubernetesHaServices.getLeaderPathForJobManager(jobID);
                            final KubernetesConfigMap configMap =
                                    new TestingFlinkKubeClient.MockKubernetesConfigMap(
                                            configMapName);
                            flinkKubeClient.createConfigMap(configMap);
                            assertThat(
                                    flinkKubeClient.getConfigMap(configMapName).isPresent(),
                                    is(true));
                            kubernetesHaServices.internalCleanupJobData(jobID);
                            assertThat(
                                    flinkKubeClient.getConfigMap(configMapName).isPresent(),
                                    is(false));
                        });
            }
        };
    }
}
