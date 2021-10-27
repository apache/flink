/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.plugin;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/** Utility functions for the plugin mechanism. */
public final class PluginUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PluginConfig.class);

    private PluginUtils() {
        throw new AssertionError("Singleton class.");
    }

    /**
     * Creates a {@link PluginManager} based on the job's {@link Configuration} and derive the
     * plugin directory from the root folder and the set environment variable {@link
     * ConfigConstants#ENV_FLINK_PLUGINS_DIR} or default to {@link
     * ConfigConstants#DEFAULT_FLINK_PLUGINS_DIRS}.
     *
     * @param configuration of the job
     * @return {@link PluginManager} to load the plugins
     */
    public static PluginManager createPluginManagerFromRootFolder(Configuration configuration) {
        final Optional<File> pluginDirOpt = getPluginsDirFromEnvironment();
        return pluginDirOpt
                .map(file -> createPluginManagerFromFolder(configuration, file.toPath()))
                .orElseGet(
                        () ->
                                createPluginManagerFromPluginConfig(
                                        new PluginConfig(
                                                Optional.empty(),
                                                CoreOptions.getPluginParentFirstLoaderPatterns(
                                                        configuration))));
    }

    /**
     * Creates a {@link PluginManager} based on the job's {@link Configuration} and takes the passed
     * {@code pluginsDir} as plugin directory.
     *
     * @param configuration of the job
     * @param pluginsDir denoting the directory to look for the plugins
     * @return {@link PluginManager} to load the plugins
     */
    public static PluginManager createPluginManagerFromFolder(
            Configuration configuration, Path pluginsDir) {
        if (!Files.exists(pluginsDir)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Plugin directory %s does not exist.", pluginsDir.toAbsolutePath()));
        }
        if (!Files.isDirectory(pluginsDir)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Plugin directory %s is not a directory.",
                            pluginsDir.toAbsolutePath()));
        }
        return createPluginManagerFromPluginConfig(
                new PluginConfig(
                        Optional.of(pluginsDir),
                        CoreOptions.getPluginParentFirstLoaderPatterns(configuration)));
    }

    private static PluginManager createPluginManagerFromPluginConfig(PluginConfig pluginConfig) {
        if (pluginConfig.getPluginsPath().isPresent()) {
            try {
                Collection<PluginDescriptor> pluginDescriptors =
                        new DirectoryBasedPluginFinder(pluginConfig.getPluginsPath().get())
                                .findPlugins();
                return new DefaultPluginManager(
                        pluginDescriptors, pluginConfig.getAlwaysParentFirstPatterns());
            } catch (IOException e) {
                throw new FlinkRuntimeException(
                        "Exception when trying to initialize plugin system.", e);
            }
        } else {
            return new DefaultPluginManager(
                    Collections.emptyList(), pluginConfig.getAlwaysParentFirstPatterns());
        }
    }

    /**
     * Tries to extract the plugin directory from the environment.
     *
     * @return if plugin directory is a directory otherwise {@link Optional#empty()}
     */
    public static Optional<File> getPluginsDirFromEnvironment() {
        String pluginsDir =
                System.getenv()
                        .getOrDefault(
                                ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS);

        File pluginsDirFile = new File(pluginsDir);
        if (!pluginsDirFile.isDirectory()) {
            LOG.warn("The plugins directory [{}] does not exist.", pluginsDirFile);
            return Optional.empty();
        }
        return Optional.of(pluginsDirFile);
    }
}
