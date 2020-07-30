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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.util.ChildFirstClassLoader;
import org.apache.flink.util.FlinkUserCodeClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.function.Consumer;

/**
 * Gives the URLClassLoader a nicer name for debugging purposes.
 */
public class FlinkUserCodeClassLoaders {

	private FlinkUserCodeClassLoaders() {

	}

	public static URLClassLoader parentFirst(
			URL[] urls,
			ClassLoader parent,
			Consumer<Throwable> classLoadingExceptionHandler,
			boolean checkClassLoaderLeak) {
		FlinkUserCodeClassLoader classLoader = new ParentFirstClassLoader(urls,	parent,	classLoadingExceptionHandler);
		return wrapWithSafetyNet(classLoader, checkClassLoaderLeak);
	}

	public static URLClassLoader childFirst(
			URL[] urls,
			ClassLoader parent,
			String[] alwaysParentFirstPatterns,
			Consumer<Throwable> classLoadingExceptionHandler,
			boolean checkClassLoaderLeak) {
		FlinkUserCodeClassLoader classLoader = new ChildFirstClassLoader(
			urls,
			parent,
			alwaysParentFirstPatterns,
			classLoadingExceptionHandler);
		return wrapWithSafetyNet(classLoader, checkClassLoaderLeak);
	}

	public static URLClassLoader create(
			ResolveOrder resolveOrder,
			URL[] urls,
			ClassLoader parent,
			String[] alwaysParentFirstPatterns,
			Consumer<Throwable> classLoadingExceptionHandler,
			boolean checkClassLoaderLeak) {

		switch (resolveOrder) {
			case CHILD_FIRST:
				return childFirst(
					urls,
					parent,
					alwaysParentFirstPatterns,
					classLoadingExceptionHandler,
					checkClassLoaderLeak);
			case PARENT_FIRST:
				return parentFirst(urls, parent, classLoadingExceptionHandler, checkClassLoaderLeak);
			default:
				throw new IllegalArgumentException("Unknown class resolution order: " + resolveOrder);
		}
	}

	private static URLClassLoader wrapWithSafetyNet(FlinkUserCodeClassLoader classLoader, boolean check) {
		return check ? new SafetyNetWrapperClassLoader(classLoader, classLoader.getParent()) : classLoader;
	}

	/**
	 * Class resolution order for Flink URL {@link ClassLoader}.
	 */
	public enum ResolveOrder {
		CHILD_FIRST, PARENT_FIRST;

		public static ResolveOrder fromString(String resolveOrder) {
			if (resolveOrder.equalsIgnoreCase("parent-first")) {
				return PARENT_FIRST;
			} else if (resolveOrder.equalsIgnoreCase("child-first")) {
				return CHILD_FIRST;
			} else {
				throw new IllegalArgumentException("Unknown resolve order: " + resolveOrder);
			}
		}
	}

	/**
	 * Regular URLClassLoader that first loads from the parent and only after that from the URLs.
	 */
	public static class ParentFirstClassLoader extends FlinkUserCodeClassLoader {

		ParentFirstClassLoader(URL[] urls, ClassLoader parent, Consumer<Throwable> classLoadingExceptionHandler) {
			super(urls, parent, classLoadingExceptionHandler);
		}

		static {
			ClassLoader.registerAsParallelCapable();
		}
	}

	/**
	 * Ensures that holding a reference on the context class loader outliving the scope of user code does not prevent
	 * the user classloader to be garbage collected (FLINK-16245).
	 *
	 * <p>This classloader delegates to the actual user classloader. Upon {@link #close()}, the delegate is nulled
	 * and can be garbage collected. Additional class resolution will be resolved solely through the bootstrap
	 * classloader and most likely result in ClassNotFound exceptions.
	 */
	private static class SafetyNetWrapperClassLoader extends URLClassLoader implements Closeable {
		private static final Logger LOG = LoggerFactory.getLogger(SafetyNetWrapperClassLoader.class);

		private volatile FlinkUserCodeClassLoader inner;

		SafetyNetWrapperClassLoader(FlinkUserCodeClassLoader inner, ClassLoader parent) {
			super(new URL[0], parent);
			this.inner = inner;
		}

		@Override
		public void close() {
			final FlinkUserCodeClassLoader inner = this.inner;
			if (inner != null) {
				try {
					inner.close();
				} catch (IOException e) {
					LOG.warn("Could not close user classloader", e);
				}
			}
			this.inner = null;
		}

		private FlinkUserCodeClassLoader ensureInner() {
			if (inner == null) {
				throw new IllegalStateException("Trying to access closed classloader. Please check if you store " +
					"classloaders directly or indirectly in static fields. If the stacktrace suggests that the leak " +
					"occurs in a third party library and cannot be fixed immediately, you can disable this check " +
					"with the configuration '" + CoreOptions.CHECK_LEAKED_CLASSLOADER.key() + "'.");
			}
			return inner;
		}

		@Override
		public Class<?> loadClass(String name) throws ClassNotFoundException {
			return ensureInner().loadClass(name);
		}

		@Override
		protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
			// called for dynamic class loading
			return ensureInner().loadClass(name, resolve);
		}

		@Override
		public URL getResource(String name) {
			return ensureInner().getResource(name);
		}

		@Override
		public Enumeration<URL> getResources(String name) throws IOException {
			return ensureInner().getResources(name);
		}

		static {
			ClassLoader.registerAsParallelCapable();
		}
	}
}
