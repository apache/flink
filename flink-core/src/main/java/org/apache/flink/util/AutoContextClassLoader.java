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

package org.apache.flink.util;

/**
 * Sets a context class loader in a "try-with-resources" pattern.
 *
 * <pre>
 * {@code
 * try (AutoContextClassLoader ignored = AutoContextClassLoader.of(classloader)) {
 *     // code that needs the context class loader
 * }
 * }
 * </pre>
 *
 * <p>This is conceptually the same as the code below.

 * <pre>
 * {@code
 * ClassLoader original = Thread.currentThread().getContextClassLoader();
 * Thread.currentThread().setContextClassLoader(classloader);
 * try {
 *     // code that needs the context class loader
 * } finally {
 *     Thread.currentThread().setContextClassLoader(original);
 * }
 * }
 * </pre>
 */
public final class AutoContextClassLoader implements AutoCloseable {

	/**
	 * Sets the context class loader to the given ClassLoader and returns a resource
	 * that sets it back to the current context ClassLoader when the resource is closed.
	 *
	 * <pre>{@code
	 * try (AutoContextClassLoader ignored = AutoContextClassLoader.of(classloader)) {
	 *     // code that needs the context class loader
	 * }
	 * }</pre>
	 */
	public static AutoContextClassLoader of(ClassLoader cl) {
		final Thread t = Thread.currentThread();
		final ClassLoader original = t.getContextClassLoader();

		t.setContextClassLoader(cl);

		return new AutoContextClassLoader(t, original);
	}

	// ------------------------------------------------------------------------

	private final Thread thread;

	private final ClassLoader originalContextClassLoader;

	private AutoContextClassLoader(Thread thread, ClassLoader originalContextClassLoader) {
		this.thread = thread;
		this.originalContextClassLoader = originalContextClassLoader;
	}

	@Override
	public void close() {
		thread.setContextClassLoader(originalContextClassLoader);
	}
}
