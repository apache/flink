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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;

import io.fabric8.kubernetes.api.model.Pod;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Tests for {@link InitTaskManagerDecorator} decorating service account. */
public class InitTaskManagerDecoratorAccountTest extends KubernetesTaskManagerTestBase {

    private static final String SERVICE_ACCOUNT_NAME = "service-test";
    private static final String TASK_MANAGER_SERVICE_ACCOUNT_NAME = "tm-service-test";

    private Pod resultPod;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        this.flinkConfig.set(
                KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, SERVICE_ACCOUNT_NAME);
        this.flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_SERVICE_ACCOUNT,
                TASK_MANAGER_SERVICE_ACCOUNT_NAME);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        final InitTaskManagerDecorator initTaskManagerDecorator =
                new InitTaskManagerDecorator(kubernetesTaskManagerParameters);

        final FlinkPod resultFlinkPod =
                initTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod);
        this.resultPod = resultFlinkPod.getPodWithoutMainContainer();
    }

    @Test
    public void testPodServiceAccountName() {
        assertThat(
                this.resultPod.getSpec().getServiceAccountName(),
                is(TASK_MANAGER_SERVICE_ACCOUNT_NAME));
    }
}
