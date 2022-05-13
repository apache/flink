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

package org.apache.flink.kubernetes;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

import org.apache.flink.shaded.guava30.com.google.common.io.Files;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/** Utilities for the Kubernetes tests. */
public class KubernetesTestUtils {

    public static void createTemporyFile(String data, File directory, String fileName)
            throws IOException {
        Files.write(data, new File(directory, fileName), StandardCharsets.UTF_8);
    }

    public static Configuration loadConfigurationFromString(String content) {
        final Configuration configuration = new Configuration();
        for (String line : content.split(System.lineSeparator())) {
            final String[] splits = line.split(":");
            if (splits.length >= 2) {
                configuration.setString(
                        splits[0].trim(), StringUtils.substringAfter(line, ":").trim());
            }
        }
        return configuration;
    }

    public static KubernetesTaskManagerParameters createTaskManagerParameters(
            Configuration flinkConfig, String podName) {
        final ContaineredTaskManagerParameters containeredTaskManagerParameters =
                ContaineredTaskManagerParameters.create(
                        flinkConfig, TaskExecutorProcessUtils.processSpecFromConfig(flinkConfig));
        return new KubernetesTaskManagerParameters(
                flinkConfig,
                podName,
                "",
                "",
                containeredTaskManagerParameters,
                Collections.emptyMap());
    }
}
