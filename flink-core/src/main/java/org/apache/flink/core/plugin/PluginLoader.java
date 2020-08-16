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
import org.apache.flink.util.TemporaryClassLoaderContext;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * A {@link PluginLoader} is used by the {@link PluginManager} to load a single plugin. It is essentially a combination
 * of a {@link PluginClassLoader} and {@link ServiceLoader}. This class can locate and load service implementations
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
		return new PluginClassLoader(
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
	public <P> Iterator<P> load(Class<P> service) {
		try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(pluginClassLoader)) {
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
			try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(pluginClassLoader)) {
				return delegate.next();
			}
		}
	}

	/**
	 * Loads all classes from the plugin jar except for explicitly white-listed packages (org.apache.flink, logging).
	 *
	 * <p>No class/resource in the system class loader (everything in lib/) can be seen in the plugin except those
	 * starting with a whitelist prefix.
	 */
	private static final class PluginClassLoader extends URLClassLoader {
		private static final ClassLoader PLATFORM_OR_BOOTSTRAP_LOADER;

		private final ClassLoader flinkClassLoader;

		private final String[] allowedFlinkPackages;

		private final String[] allowedResourcePrefixes;

		PluginClassLoader(URL[] pluginResourceURLs, ClassLoader flinkClassLoader, String[] allowedFlinkPackages) {
			super(pluginResourceURLs, PLATFORM_OR_BOOTSTRAP_LOADER);
			this.flinkClassLoader = flinkClassLoader;
			this.allowedFlinkPackages = allowedFlinkPackages;
			allowedResourcePrefixes = Arrays.stream(allowedFlinkPackages)
				.map(packageName -> packageName.replace('.', '/'))
				.toArray(String[]::new);
		}

		@Override
		protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
			synchronized (getClassLoadingLock(name)) {
				final Class<?> loadedClass = findLoadedClass(name);
				if (loadedClass != null) {
					return resolveIfNeeded(resolve, loadedClass);
				}

				if (isAllowedFlinkClass(name)) {
					try {
						return resolveIfNeeded(resolve, flinkClassLoader.loadClass(name));
					} catch (ClassNotFoundException e) {
						// fallback to resolving it in this classloader
						// for cases where the plugin uses org.apache.flink namespace
					}
				}

				return super.loadClass(name, resolve);
			}
		}

		private Class<?> resolveIfNeeded(final boolean resolve, final Class<?> loadedClass) {
			if (resolve) {
				resolveClass(loadedClass);
			}

			return loadedClass;
		}

		@Override
		public URL getResource(final String name) {
			if (isAllowedFlinkResource(name)) {
				return flinkClassLoader.getResource(name);
			}

			return super.getResource(name);
		}

		@Override
		public Enumeration<URL> getResources(final String name) throws IOException {
			// ChildFirstClassLoader merges child and parent resources
			if (isAllowedFlinkResource(name)) {
				return flinkClassLoader.getResources(name);
			}

			return super.getResources(name);
		}

		private boolean isAllowedFlinkClass(final String name) {
			return Arrays.stream(allowedFlinkPackages).anyMatch(name::startsWith);
		}

		private boolean isAllowedFlinkResource(final String name) {
			return Arrays.stream(allowedResourcePrefixes).anyMatch(name::startsWith);
		}

		static {
			ClassLoader platformLoader = null;
			try {
				platformLoader = (ClassLoader) ClassLoader.class
					.getMethod("getPlatformClassLoader")
					.invoke(null);
			} catch (NoSuchMethodException e) {
				// on Java 8 this method does not exist, but using null indicates the bootstrap loader that we want
				// to have
			} catch (Exception e) {
				throw new IllegalStateException("Cannot retrieve platform classloader on Java 9+", e);
			}
			PLATFORM_OR_BOOTSTRAP_LOADER = platformLoader;

			ClassLoader.registerAsParallelCapable();
		}
	}
}
