/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ServiceLoader;

import static org.apache.flink.shaded.guava31.com.google.common.collect.Iterators.concat;

/** A thin wrapper around {@link PluginManager} to load {@link StateChangelogStorage}. */
@Internal
public class StateChangelogStorageLoader {

    private static final Logger LOG = LoggerFactory.getLogger(StateChangelogStorageLoader.class);

    /**
     * Mapping of state changelog storage identifier to the corresponding storage factories,
     * populated in {@link StateChangelogStorageLoader#initialize(PluginManager)}.
     */
    private static final HashMap<String, StateChangelogStorageFactory>
            STATE_CHANGELOG_STORAGE_FACTORIES = new HashMap<>();

    static {
        // Guarantee to trigger once.
        initialize(null);
    }

    public static void initialize(PluginManager pluginManager) {
        STATE_CHANGELOG_STORAGE_FACTORIES.clear();
        Iterator<StateChangelogStorageFactory> iterator =
                pluginManager == null
                        ? ServiceLoader.load(StateChangelogStorageFactory.class).iterator()
                        : concat(
                                pluginManager.load(StateChangelogStorageFactory.class),
                                ServiceLoader.load(StateChangelogStorageFactory.class).iterator());
        iterator.forEachRemaining(
                factory -> {
                    String identifier = factory.getIdentifier().toLowerCase();
                    StateChangelogStorageFactory prev =
                            STATE_CHANGELOG_STORAGE_FACTORIES.get(identifier);
                    if (prev == null) {
                        STATE_CHANGELOG_STORAGE_FACTORIES.put(identifier, factory);
                    } else {
                        LOG.warn(
                                "StateChangelogStorageLoader found duplicated factory,"
                                        + " using {} instead of {} for name {}.",
                                prev.getClass().getName(),
                                factory.getClass().getName(),
                                identifier);
                    }
                });
        LOG.info(
                "StateChangelogStorageLoader initialized with shortcut names {{}}.",
                String.join(",", STATE_CHANGELOG_STORAGE_FACTORIES.keySet()));
    }

    @Nullable
    public static StateChangelogStorage<?> load(
            JobID jobID,
            Configuration configuration,
            TaskManagerJobMetricGroup metricGroup,
            LocalRecoveryConfig localRecoveryConfig)
            throws IOException {
        final String identifier =
                configuration
                        .getString(StateChangelogOptions.STATE_CHANGE_LOG_STORAGE)
                        .toLowerCase();

        StateChangelogStorageFactory factory = STATE_CHANGELOG_STORAGE_FACTORIES.get(identifier);
        if (factory == null) {
            LOG.warn("Cannot find a factory for changelog storage with name '{}'.", identifier);
            return null;
        } else {
            LOG.info("Creating a changelog storage with name '{}'.", identifier);
            return factory.createStorage(jobID, configuration, metricGroup, localRecoveryConfig);
        }
    }

    @Nonnull
    public static StateChangelogStorageView<?> loadFromStateHandle(
            Configuration configuration, ChangelogStateHandle changelogStateHandle)
            throws IOException {
        StateChangelogStorageFactory factory =
                STATE_CHANGELOG_STORAGE_FACTORIES.get(changelogStateHandle.getStorageIdentifier());
        if (factory == null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Cannot find a factory for changelog storage with name '%s' to restore from '%s'.",
                            changelogStateHandle.getStorageIdentifier(),
                            changelogStateHandle.getClass().getSimpleName()));
        } else {
            LOG.info(
                    "Creating a changelog storage with name '{}' to restore from '{}'.",
                    changelogStateHandle.getStorageIdentifier(),
                    changelogStateHandle.getClass().getSimpleName());
            return factory.createStorageView(configuration);
        }
    }
}
