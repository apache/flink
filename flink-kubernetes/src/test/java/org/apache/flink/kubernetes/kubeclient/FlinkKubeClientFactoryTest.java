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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link FlinkKubeClientFactory}. */
public class FlinkKubeClientFactoryTest extends TestLogger {

    @Test
    public void testTrySetMaxConcurrentRequestViaJavaSystemProperty() {
        System.setProperty(Config.KUBERNETES_MAX_CONCURRENT_REQUESTS, "100");
        testTrySetMaxConcurrentRequest(100);
        System.getProperties().remove(Config.KUBERNETES_MAX_CONCURRENT_REQUESTS);
    }

    @Test
    public void testTrySetMaxConcurrentRequestViaEnvVar() {
        final Map<String, String> currentEnv = System.getenv();
        final String envKey = "KUBERNETES_MAX_CONCURRENT_REQUESTS";
        final Map<String, String> envMap = new HashMap<>();
        envMap.put(envKey, "200");
        CommonTestUtils.setEnv(envMap, true);
        testTrySetMaxConcurrentRequest(200);
        CommonTestUtils.setEnv(currentEnv);
    }

    private void testTrySetMaxConcurrentRequest(int expectedValue) {
        final Config config = new ConfigBuilder().build();
        FlinkKubeClientFactory.trySetMaxConcurrentRequest(config);
        assertThat(config.getMaxConcurrentRequests(), is(expectedValue));
    }
}
