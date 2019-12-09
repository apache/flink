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
import org.apache.flink.util.ArrayUtils;
import org.apache.flink.util.ChildFirstClassLoader;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * A {@link PluginLoader} is used by the {@link PluginManager} to load a single plugin. It is essentially a combination
 * of a {@link ChildFirstClassLoader} and {@link ServiceLoader}. This class can locate and load service implementations
 * from the plugin for a given SPI. The {@link PluginDescriptor}, which among other information contains the resource
 * URLs, is provided at construction.
 */
@ThreadSafe
public class PluginLoader {

	/** Classloader which is used to load the plugin classes. We expect this classloader is thread-safe.*/
	private final ClassLoader pluginClassLoader;

	@VisibleForTesting
	public PluginLoader(ClassLoader pluginClassLoader) {
		this.pluginClassLoader = pluginClassLoader;
	}

	@VisibleForTesting
	public static ClassLoader createPluginClassLoader(PluginDescriptor pluginDescriptor, ClassLoader parentClassLoader, String[] alwaysParentFirstPatterns) {
		return new ChildFirstClassLoader(
			pluginDescriptor.getPluginResourceURLs(),
			parentClassLoader,
			ArrayUtils.concat(alwaysParentFirstPatterns, pluginDescriptor.getLoaderExcludePatterns()));
	}

	public static PluginLoader create(PluginDescriptor pluginDescriptor, ClassLoader parentClassLoader, String[] alwaysParentFirstPatterns) {
		return new PluginLoader(createPluginClassLoader(pluginDescriptor, parentClassLoader, alwaysParentFirstPatterns));
	}

	/**
	 * Returns in iterator over all available implementations of the given service interface (SPI) for the plugin.
	 *
	 * @param service the service interface (SPI) for which implementations are requested.
	 * @param <P> Type of the requested plugin service.
	 * @return An iterator of all implementations of the given service interface that could be loaded from the plugin.
	 */
	public <P extends Plugin> Iterator<P> load(Class<P> service) {
		try (TemporaryClassLoaderContext classLoaderContext = new TemporaryClassLoaderContext(pluginClassLoader)) {
			return new ContextClassLoaderSettingIterator<>(
				ServiceLoader.load(service, pluginClassLoader).iterator(),
				pluginClassLoader);
		}
	}

	/**
	 * Wrapper for the service iterator. The wrapper will set/unset the context classloader to the plugin classloader
	 * around the point where elements are returned.
	 *
	 * @param <P> type of the iterated plugin element.
	 */
	static class ContextClassLoaderSettingIterator<P extends Plugin> implements Iterator<P> {

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
			try (TemporaryClassLoaderContext classLoaderContext = new TemporaryClassLoaderContext(pluginClassLoader)) {
				return delegate.next();
			}
		}
	}

}
