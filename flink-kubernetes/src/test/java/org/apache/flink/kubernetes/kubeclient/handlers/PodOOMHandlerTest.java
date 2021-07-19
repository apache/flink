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

package org.apache.flink.kubernetes.kubeclient.handlers;

import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import io.fabric8.kubernetes.api.model.ContainerStateBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/** Test for PodOOMHandler. */
public class PodOOMHandlerTest {

    @Test
    public void testOOMCount() {
        PodOOMHandler podOOMHandler =
                new PodOOMHandler(
                        UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());

        String finishTime = "2020-03-19T13:14:48Z";
        KubernetesPod kubernetesPod =
                new KubernetesPod(
                        new PodBuilder()
                                .editOrNewMetadata()
                                .withName("pod-1")
                                .endMetadata()
                                .withSpec(
                                        new PodSpecBuilder()
                                                .withContainers(Collections.emptyList())
                                                .build())
                                .withStatus(
                                        new PodStatusBuilder()
                                                .withContainerStatuses(
                                                        new ContainerStatus[] {
                                                            new ContainerStatusBuilder()
                                                                    .withState(
                                                                            new ContainerStateBuilder()
                                                                                    .withNewTerminated()
                                                                                    .withFinishedAt(
                                                                                            finishTime)
                                                                                    .withReason(
                                                                                            "OOMKilled")
                                                                                    .endTerminated()
                                                                                    .build())
                                                                    .build()
                                                        })
                                                .build())
                                .build());
        podOOMHandler.handle(kubernetesPod);
        Assert.assertEquals(1, podOOMHandler.getTombstones().size());
        Assert.assertTrue(podOOMHandler.getTombstones().getIfPresent(finishTime).contains("pod-1"));
        // test ignore same events
        podOOMHandler.handle(kubernetesPod);
        Assert.assertEquals(1, podOOMHandler.getTombstones().getIfPresent(finishTime).size());

        kubernetesPod.getInternalResource().getMetadata().setName("pod-2");
        podOOMHandler.handle(kubernetesPod);
        Assert.assertEquals(2, podOOMHandler.getTombstones().getIfPresent(finishTime).size());
    }
}
