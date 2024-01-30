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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.guava31.com.google.common.base.Joiner;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** Default implementation of {@link PluginManager}. */
@Internal
@ThreadSafe
public class DefaultPluginManager implements PluginManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPluginManager.class);

    /**
     * Parent-classloader to all classloader that are used for plugin loading. We expect that this
     * is thread-safe.
     */
    private final ClassLoader parentClassLoader;

    /** A collection of descriptions of all plugins known to this plugin manager. */
    private final Collection<PluginDescriptor> pluginDescriptors;

    private final Lock pluginLoadersLock;

    @GuardedBy("pluginLoadersLock")
    private final Map<String, PluginLoader> pluginLoaders;

    /** List of patterns for classes that should always be resolved from the parent ClassLoader. */
    private final String[] alwaysParentFirstPatterns;

    @VisibleForTesting
    DefaultPluginManager() {
        parentClassLoader = null;
        pluginDescriptors = null;
        pluginLoadersLock = null;
        pluginLoaders = null;
        alwaysParentFirstPatterns = null;
    }

    public DefaultPluginManager(
            Collection<PluginDescriptor> pluginDescriptors, String[] alwaysParentFirstPatterns) {
        this(
                pluginDescriptors,
                DefaultPluginManager.class.getClassLoader(),
                alwaysParentFirstPatterns);
    }

    public DefaultPluginManager(
            Collection<PluginDescriptor> pluginDescriptors,
            ClassLoader parentClassLoader,
            String[] alwaysParentFirstPatterns) {
        this.pluginDescriptors = pluginDescriptors;
        this.pluginLoadersLock = new ReentrantLock();
        this.pluginLoaders = new HashMap<>();
        this.parentClassLoader = parentClassLoader;
        this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
    }

    @Override
    public <P> Iterator<P> load(Class<P> service) {
        ArrayList<Iterator<P>> combinedIterators = new ArrayList<>(pluginDescriptors.size());
        for (PluginDescriptor pluginDescriptor : pluginDescriptors) {
            PluginLoader pluginLoader;
            String pluginId = pluginDescriptor.getPluginId();
            pluginLoadersLock.lock();
            try {
                if (pluginLoaders.containsKey(pluginId)) {
                    LOG.info("Plugin loader with ID found, reusing it: {}", pluginId);
                    pluginLoader = pluginLoaders.get(pluginId);
                } else {
                    LOG.info("Plugin loader with ID not found, creating it: {}", pluginId);
                    pluginLoader =
                            PluginLoader.create(
                                    pluginDescriptor, parentClassLoader, alwaysParentFirstPatterns);
                    pluginLoaders.putIfAbsent(pluginId, pluginLoader);
                }
            } finally {
                pluginLoadersLock.unlock();
            }
            combinedIterators.add(pluginLoader.load(service));
        }
        return Iterators.concat(combinedIterators.iterator());
    }

    @Override
    public String toString() {
        return "PluginManager{"
                + "parentClassLoader="
                + parentClassLoader
                + ", pluginDescriptors="
                + pluginDescriptors
                + ", pluginLoaders="
                + Joiner.on(",").withKeyValueSeparator("=").join(pluginLoaders)
                + ", alwaysParentFirstPatterns="
                + Arrays.toString(alwaysParentFirstPatterns)
                + '}';
    }
}
