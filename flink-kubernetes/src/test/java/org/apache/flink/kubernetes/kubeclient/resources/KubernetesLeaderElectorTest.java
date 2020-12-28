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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;

import org.junit.Test;

import java.util.UUID;

import static org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector.LEADER_ANNOTATION_KEY;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link KubernetesLeaderElector}. */
public class KubernetesLeaderElectorTest extends KubernetesTestBase {

    private String lockIdentity;
    private KubernetesConfigMap leaderConfigMap;

    private static final String CONFIGMAP_NAME = "test-config-map";

    public void onSetup() {
        lockIdentity = UUID.randomUUID().toString();
        leaderConfigMap = new TestingFlinkKubeClient.MockKubernetesConfigMap(CONFIGMAP_NAME);
    }

    @Test
    public void testNoAnnotation() {
        assertThat(KubernetesLeaderElector.hasLeadership(leaderConfigMap, lockIdentity), is(false));
    }

    @Test
    public void testAnnotationNotMatch() {
        leaderConfigMap.getAnnotations().put(LEADER_ANNOTATION_KEY, "wrong lock");
        assertThat(KubernetesLeaderElector.hasLeadership(leaderConfigMap, lockIdentity), is(false));
    }

    @Test
    public void testAnnotationMatched() {
        leaderConfigMap
                .getAnnotations()
                .put(LEADER_ANNOTATION_KEY, "other information " + lockIdentity);
        assertThat(KubernetesLeaderElector.hasLeadership(leaderConfigMap, lockIdentity), is(true));
    }
}
