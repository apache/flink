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

package org.apache.flink.core.plugin;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.classloading.ComponentClassLoader;
import org.apache.flink.util.ArrayUtils;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * A {@link PluginLoader} is used by the {@link PluginManager} to load a single plugin. It is
 * essentially a combination of a {@link PluginClassLoader} and {@link ServiceLoader}. This class
 * can locate and load service implementations from the plugin for a given SPI. The {@link
 * PluginDescriptor}, which among other information contains the resource URLs, is provided at
 * construction.
 */
@ThreadSafe
public class PluginLoader implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PluginLoader.class);

    private final String pluginId;

    /**
     * Classloader which is used to load the plugin classes. We expect this classloader is
     * thread-safe.
     */
    private final URLClassLoader pluginClassLoader;

    @VisibleForTesting
    public PluginLoader(String pluginId, URLClassLoader pluginClassLoader) {
        this.pluginId = pluginId;
        this.pluginClassLoader = pluginClassLoader;
    }

    @VisibleForTesting
    public static URLClassLoader createPluginClassLoader(
            PluginDescriptor pluginDescriptor,
            ClassLoader parentClassLoader,
            String[] alwaysParentFirstPatterns) {
        return new PluginClassLoader(
                pluginDescriptor.getPluginResourceURLs(),
                parentClassLoader,
                ArrayUtils.concat(
                        alwaysParentFirstPatterns, pluginDescriptor.getLoaderExcludePatterns()));
    }

    public static PluginLoader create(
            PluginDescriptor pluginDescriptor,
            ClassLoader parentClassLoader,
            String[] alwaysParentFirstPatterns) {
        return new PluginLoader(
                pluginDescriptor.getPluginId(),
                createPluginClassLoader(
                        pluginDescriptor, parentClassLoader, alwaysParentFirstPatterns));
    }

    /**
     * Returns in iterator over all available implementations of the given service interface (SPI)
     * for the plugin.
     *
     * @param service the service interface (SPI) for which implementations are requested.
     * @param <P> Type of the requested plugin service.
     * @return An iterator of all implementations of the given service interface that could be
     *     loaded from the plugin.
     */
    public <P> Iterator<P> load(Class<P> service) {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(pluginClassLoader)) {
            return new ContextClassLoaderSettingIterator<>(
                    ServiceLoader.load(service, pluginClassLoader).iterator(), pluginClassLoader);
        }
    }

    @Override
    public void close() {
        try {
            pluginClassLoader.close();
        } catch (IOException e) {
            LOG.warn("An error occurred while closing the classloader for plugin {}.", pluginId);
        }
    }

    /**
     * Wrapper for the service iterator. The wrapper will set/unset the context classloader to the
     * plugin classloader around the point where elements are returned.
     *
     * @param <P> type of the iterated plugin element.
     */
    static class ContextClassLoaderSettingIterator<P> implements Iterator<P> {

        private final Iterator<P> delegate;
        private final ClassLoader pluginClassLoader;

        ContextClassLoaderSettingIterator(Iterator<P> delegate, ClassLoader pluginClassLoader) {
            this.delegate = delegate;
            this.pluginClassLoader = pluginClassLoader;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public P next() {
            try (TemporaryClassLoaderContext ignored =
                    TemporaryClassLoaderContext.of(pluginClassLoader)) {
                return delegate.next();
            }
        }
    }

    /**
     * Loads all classes from the plugin jar except for explicitly white-listed packages
     * (org.apache.flink, logging).
     *
     * <p>No class/resource in the system class loader (everything in lib/) can be seen in the
     * plugin except those starting with a whitelist prefix.
     */
    private static final class PluginClassLoader extends ComponentClassLoader {

        PluginClassLoader(
                URL[] pluginResourceURLs,
                ClassLoader flinkClassLoader,
                String[] allowedFlinkPackages) {
            super(pluginResourceURLs, flinkClassLoader, allowedFlinkPackages, new String[0]);
        }
    }
}
