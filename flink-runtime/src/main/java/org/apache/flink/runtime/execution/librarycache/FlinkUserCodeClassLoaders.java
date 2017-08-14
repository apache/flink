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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Gives the URLClassLoader a nicer name for debugging purposes.
 */
public class FlinkUserCodeClassLoaders {

	public static URLClassLoader parentFirst(URL[] urls) {
		return new ParentFirstClassLoader(urls);
	}

	public static URLClassLoader parentFirst(URL[] urls, ClassLoader parent) {
		return new ParentFirstClassLoader(urls, parent);
	}

	public static URLClassLoader childFirst(URL[] urls, ClassLoader parent) {
		return new ChildFirstClassLoader(urls, parent);
	}

	public static URLClassLoader fromConfiguration(
		Configuration config, URL[] urls, ClassLoader parent) {

		String resolveOrder = config.getString(
			CoreOptions.CLASSLOADER_RESOLVE_ORDER,
			CoreOptions.CLASSLOADER_RESOLVE_ORDER.defaultValue());

		if (resolveOrder.equals("child-first")) {
			return childFirst(urls, parent);
		} else  if (resolveOrder.equals("parent-first")) {
			return parentFirst(urls, parent);
		} else {
			throw new IllegalArgumentException("Unkown class resolution order: " + resolveOrder);
		}
	}

	/**
	 * Regular URLClassLoader that first loads from the parent and only after that form the URLs.
	 */
	static class ParentFirstClassLoader extends URLClassLoader {

		public ParentFirstClassLoader(URL[] urls) {
			this(urls, FlinkUserCodeClassLoaders.class.getClassLoader());
		}

		public ParentFirstClassLoader(URL[] urls, ClassLoader parent) {
			super(urls, parent);
		}
	}

	/**
	 * A variant of the URLClassLoader that first loads from the URLs and only after that from the parent.
	 */
	static final class ChildFirstClassLoader extends URLClassLoader {

		private final ClassLoader parent;

		public ChildFirstClassLoader(URL[] urls, ClassLoader parent) {
			super(urls, null);
			this.parent = parent;
		}

		@Override
		public Class<?> findClass(String name) throws ClassNotFoundException {
			// first try to load from the URLs
			// because the URLClassLoader's parent is null, this cannot implicitly load from the parent
			try {
				return super.findClass(name);
			}
			catch (ClassNotFoundException e) {
				// not in the URL, check the parent
				return parent.loadClass(name);
			}
		}
	}

}
