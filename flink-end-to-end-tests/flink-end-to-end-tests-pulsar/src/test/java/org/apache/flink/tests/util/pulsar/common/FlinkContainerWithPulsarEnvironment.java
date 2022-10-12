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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.testframe.container.FlinkContainerTestEnvironment;
import org.apache.flink.test.resources.ResourceTestUtils;

/** A Flink Container which would bundles pulsar connector in its classpath. */
public class FlinkContainerWithPulsarEnvironment extends FlinkContainerTestEnvironment {

    public FlinkContainerWithPulsarEnvironment(int numTaskManagers, int numSlotsPerTaskManager) {
        super(
                flinkConfiguration(),
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
                resourcePath("jaxb-api.jar"),
                resourcePath("jul-to-slf4j.jar"),
                resourcePath("flink-connector-testing.jar"));
    }

    private static String resourcePath(String jarName) {
        return ResourceTestUtils.getResource(jarName).toAbsolutePath().toString();
    }

    private static Configuration flinkConfiguration() {
        Configuration configuration = new Configuration();

        // Increase the jvm metaspace memory to avoid java.lang.OutOfMemoryError: Metaspace
        configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(2048));
        configuration.set(TaskManagerOptions.JVM_METASPACE, MemorySize.ofMebiBytes(512));
        configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(2048));
        configuration.set(JobManagerOptions.JVM_METASPACE, MemorySize.ofMebiBytes(512));

        return configuration;
    }
}
