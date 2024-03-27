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
import org.apache.flink.configuration.YamlParserUtils;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

import org.apache.flink.shaded.guava31.com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Utilities for the Kubernetes tests. */
public class KubernetesTestUtils {

    public static void createTemporyFile(String data, File directory, String fileName)
            throws IOException {
        Files.write(data, new File(directory, fileName), StandardCharsets.UTF_8);
    }

    public static Configuration loadConfigurationFromString(String content) {
        Map<String, Object> map = YamlParserUtils.convertToObject(content, Map.class);
        return Configuration.fromMap(flatten(map, ""));
    }

    private static Map<String, String> flatten(Map<String, Object> config, String keyPrefix) {
        final Map<String, String> flattenedMap = new HashMap<>();

        config.forEach(
                (key, value) -> {
                    String flattenedKey = keyPrefix + key;
                    if (value instanceof Map) {
                        Map<String, Object> e = (Map<String, Object>) value;
                        flattenedMap.putAll(flatten(e, flattenedKey + "."));
                    } else {
                        flattenedMap.put(flattenedKey, YamlParserUtils.toYAMLString(value));
                    }
                });

        return flattenedMap;
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
                Collections.emptyMap(),
                Collections.emptySet());
    }
}
