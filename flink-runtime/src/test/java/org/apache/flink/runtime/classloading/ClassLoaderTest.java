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

package org.apache.flink.runtime.classloading;

import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests for classloading and class loder utilities.
 */
public class ClassLoaderTest extends TestLogger {

	@Test
	public void testParentFirstClassLoading() throws Exception {
		final ClassLoader parentClassLoader = getClass().getClassLoader();

		// collect the libraries / class folders with RocksDB related code: the state backend and RocksDB itself
		final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

		final URLClassLoader childClassLoader1 = FlinkUserCodeClassLoaders.parentFirst(
				new URL[] { childCodePath }, parentClassLoader);

		final URLClassLoader childClassLoader2 = FlinkUserCodeClassLoaders.parentFirst(
				new URL[] { childCodePath }, parentClassLoader);

		final String className = ClassLoaderTest.class.getName();

		final Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
		final Class<?> clazz2 = Class.forName(className, false, childClassLoader1);
		final Class<?> clazz3 = Class.forName(className, false, childClassLoader2);

		assertEquals(clazz1, clazz2);
		assertEquals(clazz1, clazz3);

		childClassLoader1.close();
		childClassLoader2.close();
	}

	@Test
	public void testChildFirstClassLoading() throws Exception {
		final ClassLoader parentClassLoader = getClass().getClassLoader();

		// collect the libraries / class folders with RocksDB related code: the state backend and RocksDB itself
		final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

		final URLClassLoader childClassLoader1 = FlinkUserCodeClassLoaders.childFirst(
				new URL[] { childCodePath }, parentClassLoader, new String[0]);

		final URLClassLoader childClassLoader2 = FlinkUserCodeClassLoaders.childFirst(
				new URL[] { childCodePath }, parentClassLoader, new String[0]);

		final String className = ClassLoaderTest.class.getName();

		final Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
		final Class<?> clazz2 = Class.forName(className, false, childClassLoader1);
		final Class<?> clazz3 = Class.forName(className, false, childClassLoader2);

		assertNotEquals(clazz1, clazz2);
		assertNotEquals(clazz1, clazz3);
		assertNotEquals(clazz2, clazz3);

		childClassLoader1.close();
		childClassLoader2.close();
	}

	@Test
	public void testRepeatedChildFirstClassLoading() throws Exception {
		final ClassLoader parentClassLoader = getClass().getClassLoader();

		// collect the libraries / class folders with RocksDB related code: the state backend and RocksDB itself
		final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

		final URLClassLoader childClassLoader = FlinkUserCodeClassLoaders.childFirst(
				new URL[] { childCodePath }, parentClassLoader, new String[0]);

		final String className = ClassLoaderTest.class.getName();

		final Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
		final Class<?> clazz2 = Class.forName(className, false, childClassLoader);
		final Class<?> clazz3 = Class.forName(className, false, childClassLoader);
		final Class<?> clazz4 = Class.forName(className, false, childClassLoader);

		assertNotEquals(clazz1, clazz2);

		assertEquals(clazz2, clazz3);
		assertEquals(clazz2, clazz4);

		childClassLoader.close();
	}

	@Test
	public void testRepeatedParentFirstPatternClass() throws Exception {
		final String className = ClassLoaderTest.class.getName();
		final String parentFirstPattern = className.substring(0, className.lastIndexOf('.'));

		final ClassLoader parentClassLoader = getClass().getClassLoader();

		// collect the libraries / class folders with RocksDB related code: the state backend and RocksDB itself
		final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

		final URLClassLoader childClassLoader = FlinkUserCodeClassLoaders.childFirst(
				new URL[] { childCodePath }, parentClassLoader, new String[] { parentFirstPattern });

		final Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
		final Class<?> clazz2 = Class.forName(className, false, childClassLoader);
		final Class<?> clazz3 = Class.forName(className, false, childClassLoader);
		final Class<?> clazz4 = Class.forName(className, false, childClassLoader);

		assertEquals(clazz1, clazz2);
		assertEquals(clazz1, clazz3);
		assertEquals(clazz1, clazz4);

		childClassLoader.close();
	}
}
