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

package org.apache.flink.tests.util.pulsar.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.FlinkContainerTestEnvironment;

import static org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY;

/** A Flink Container which would bundles pulsar connector in its classpath. */
public class FlinkContainerWithPulsarEnvironment extends FlinkContainerTestEnvironment {

    public FlinkContainerWithPulsarEnvironment(int numTaskManagers, int numSlotsPerTaskManager) {
        super(
                numTaskManagers,
                numSlotsPerTaskManager,
                resourcePath("pulsar-connector.jar"),
                resourcePath("pulsar-client-all.jar"),
                resourcePath("pulsar-client-api.jar"),
                resourcePath("pulsar-admin-api.jar"),
                resourcePath("bouncy-castle-bc.jar"),
                resourcePath("bcpkix-jdk15on.jar"),
                resourcePath("bcprov-jdk15on.jar"),
                resourcePath("bcutil-jdk15on.jar"),
                resourcePath("bcprov-ext-jdk15on.jar"),
                resourcePath("jul-to-slf4j.jar"));
    }

    private static String resourcePath(String jarName) {
        return TestUtils.getResource(jarName).toAbsolutePath().toString();
    }

    @Override
    protected Configuration flinkConfiguration() {
        Configuration configuration = super.flinkConfiguration();
        // Increase the off heap memory for avoiding direct buffer memory error on Pulsar e2e tests.
        configuration.set(TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(100));

        return configuration;
    }
}
