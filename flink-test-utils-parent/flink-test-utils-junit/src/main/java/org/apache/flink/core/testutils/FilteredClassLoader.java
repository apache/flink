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

package org.apache.flink.core.testutils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

/**
 * A ClassLoader that filters out certain classes (by name) and throws a ClassNotFoundException
 * when they should be loaded.
 *
 * <p>This utility is useful when trying to eliminate certain classes from a class loader
 * force loading them through another class loader.
 */
public class FilteredClassLoader extends ClassLoader {

	/** The set of class names for the filtered classes. */
	private final HashSet<String> filteredClassNames;

	/**
	 * Creates a new filtered classloader.
	 *
	 * @param delegate The class loader that is filtered by this classloader.
	 * @param filteredClassNames The class names to filter out.
	 */
	public FilteredClassLoader(ClassLoader delegate, String... filteredClassNames) {
		super(Objects.requireNonNull(delegate));

		this.filteredClassNames = new HashSet<>(Arrays.asList(filteredClassNames));
	}

	@Override
	protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		synchronized (this) {
			if (filteredClassNames.contains(name)) {
				throw new ClassNotFoundException(name);
			}
			else {
				return super.loadClass(name, resolve);
			}
		}
	}
}
