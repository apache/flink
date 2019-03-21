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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

/**
 * Gives the URLClassLoader a nicer name for debugging purposes.
 */
public class FlinkUserCodeClassLoaders {

	public static URLClassLoader parentFirst(URL[] urls, ClassLoader parent) {
		return new ParentFirstClassLoader(urls, parent);
	}

	public static URLClassLoader childFirst(
		URL[] urls,
		ClassLoader parent,
		String[] alwaysParentFirstPatterns) {
		return new ChildFirstClassLoader(urls, parent, alwaysParentFirstPatterns);
	}

	public static URLClassLoader create(
		ResolveOrder resolveOrder, URL[] urls, ClassLoader parent, String[] alwaysParentFirstPatterns) {

		switch (resolveOrder) {
			case CHILD_FIRST:
				return childFirst(urls, parent, alwaysParentFirstPatterns);
			case PARENT_FIRST:
				return parentFirst(urls, parent);
			default:
				throw new IllegalArgumentException("Unknown class resolution order: " + resolveOrder);
		}
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
	static class ParentFirstClassLoader extends URLClassLoader {

		ParentFirstClassLoader(URL[] urls) {
			this(urls, FlinkUserCodeClassLoaders.class.getClassLoader());
		}

		ParentFirstClassLoader(URL[] urls, ClassLoader parent) {
			super(urls, parent);
		}
	}

	/**
	 * A variant of the URLClassLoader that first loads from the URLs and only after that from the parent.
	 *
	 * <p>{@link #getResourceAsStream(String)} uses {@link #getResource(String)} internally so we
	 * don't override that.
	 */
	static final class ChildFirstClassLoader extends URLClassLoader {

		/**
		 * The classes that should always go through the parent ClassLoader. This is relevant
		 * for Flink classes, for example, to avoid loading Flink classes that cross the
		 * user-code/system-code barrier in the user-code ClassLoader.
		 */
		private final String[] alwaysParentFirstPatterns;

		public ChildFirstClassLoader(URL[] urls, ClassLoader parent, String[] alwaysParentFirstPatterns) {
			super(urls, parent);
			this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
		}

		@Override
		protected synchronized Class<?> loadClass(
			String name, boolean resolve) throws ClassNotFoundException {

			// First, check if the class has already been loaded
			Class<?> c = findLoadedClass(name);

			if (c == null) {
				// check whether the class should go parent-first
				for (String alwaysParentFirstPattern : alwaysParentFirstPatterns) {
					if (name.startsWith(alwaysParentFirstPattern)) {
						return super.loadClass(name, resolve);
					}
				}

				try {
					// check the URLs
					c = findClass(name);
				} catch (ClassNotFoundException e) {
					// let URLClassLoader do it, which will eventually call the parent
					c = super.loadClass(name, resolve);
				}
			}

			if (resolve) {
				resolveClass(c);
			}

			return c;
		}

		@Override
		public URL getResource(String name) {
			// first, try and find it via the URLClassloader
			URL urlClassLoaderResource = findResource(name);

			if (urlClassLoaderResource != null) {
				return urlClassLoaderResource;
			}

			// delegate to super
			return super.getResource(name);
		}

		@Override
		public Enumeration<URL> getResources(String name) throws IOException {
			// first get resources from URLClassloader
			Enumeration<URL> urlClassLoaderResources = findResources(name);

			final List<URL> result = new ArrayList<>();

			while (urlClassLoaderResources.hasMoreElements()) {
				result.add(urlClassLoaderResources.nextElement());
			}

			// get parent urls
			Enumeration<URL> parentResources = getParent().getResources(name);

			while (parentResources.hasMoreElements()) {
				result.add(parentResources.nextElement());
			}

			return new Enumeration<URL>() {
				Iterator<URL> iter = result.iterator();

				public boolean hasMoreElements() {
					return iter.hasNext();
				}

				public URL nextElement() {
					return iter.next();
				}
			};
		}
	}
}
