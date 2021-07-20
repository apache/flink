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

import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/** General tests for the {@link CmdJobManagerDecorator}. */
public class CmdJobManagerDecoratorTest extends KubernetesJobManagerTestBase {

    private CmdJobManagerDecorator cmdJobManagerDecorator;

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        this.cmdJobManagerDecorator = new CmdJobManagerDecorator(kubernetesJobManagerParameters);
    }

    @Test
    public void testContainerIsDecorated() {
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        final FlinkPod resultFlinkPod = cmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod);
        assertThat(
                resultFlinkPod.getPodWithoutMainContainer(),
                is(equalTo(baseFlinkPod.getPodWithoutMainContainer())));
        assertThat(
                resultFlinkPod.getMainContainer(), not(equalTo(baseFlinkPod.getMainContainer())));
    }

    @Test
    public void testSessionClusterCommandsAndArgs() {
        testJobManagerCommandsAndArgs(KubernetesDeploymentTarget.SESSION.getName());
    }

    @Test
    public void testApplicationClusterCommandsAndArgs() {
        testJobManagerCommandsAndArgs(KubernetesDeploymentTarget.APPLICATION.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedDeploymentTargetShouldFail() {
        testJobManagerCommandsAndArgs("unsupported-deployment-target");
    }

    private void testJobManagerCommandsAndArgs(String target) {
        flinkConfig.set(DeploymentOptions.TARGET, target);
        final FlinkPod resultFlinkPod = cmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod);
        final String entryCommand = flinkConfig.get(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH);
        assertThat(
                resultFlinkPod.getMainContainer().getCommand(), containsInAnyOrder(entryCommand));
        List<String> flinkCommands =
                KubernetesUtils.getStartCommandWithBashWrapper(
                        Constants.KUBERNETES_JOB_MANAGER_SCRIPT_PATH + " " + target);
        assertThat(resultFlinkPod.getMainContainer().getArgs(), contains(flinkCommands.toArray()));
    }
}
