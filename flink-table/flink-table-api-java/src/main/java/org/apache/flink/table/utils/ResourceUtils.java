/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.utils;

import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.MutableURLClassLoader;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Utilities for register resource. */
public class ResourceUtils {

    /**
     * This method is used to register the jars which is configured by pipeline.jars option in
     * {@link Configuration} to {@link ResourceManager}.
     */
    public static void registerPipelineJars(
            Configuration configuration, ResourceManager resourceManager) {
        Set<String> jarsInConfig =
                new HashSet<>(
                        ConfigUtils.decodeListFromConfig(
                                configuration, PipelineOptions.JARS, String::toString));
        List<ResourceUri> resourceUris =
                jarsInConfig.stream()
                        .map(jarPath -> new ResourceUri(ResourceType.JAR, jarPath))
                        .collect(Collectors.toList());
        try {
            resourceManager.registerResource(resourceUris);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to register pipeline.jars [%s] in configuration to ResourceManager.",
                            jarsInConfig),
                    e);
        }
    }

    /** The tool method to create {@link ResourceManager}. */
    public static ResourceManager createResourceManager(
            URL[] urls, ClassLoader parent, Configuration configuration) {
        MutableURLClassLoader mutableURLClassLoader =
                MutableURLClassLoader.newInstance(urls, parent, configuration);
        return new ResourceManager(configuration, mutableURLClassLoader);
    }

    /** Private constructor to prevent instantiation. */
    private ResourceUtils() {
        throw new RuntimeException();
    }
}
