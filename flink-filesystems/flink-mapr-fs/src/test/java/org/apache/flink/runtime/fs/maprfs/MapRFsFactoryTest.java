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

package org.apache.flink.runtime.fs.maprfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URLClassLoader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the MapRFsFactory.
 */
public class MapRFsFactoryTest extends TestLogger {

	/**
	 * This test validates that the factory can be instantiated and configured even
	 * when MapR and Hadoop classes are missing from the classpath.
	 */
	@Test
	public void testInstantiationWithoutMapRClasses() throws Exception {
		// we do reflection magic here to instantiate the test in another class
		// loader, to make sure no MapR and Hadoop classes are in the classpath

		final String testClassName = "org.apache.flink.runtime.fs.maprfs.MapRFreeTests";

		URLClassLoader parent = (URLClassLoader) getClass().getClassLoader();
		ClassLoader maprFreeClassLoader = new MapRFreeClassLoader(parent);
		Class<?> testClass = Class.forName(testClassName, false, maprFreeClassLoader);
		Method m = testClass.getDeclaredMethod("test");

		try {
			m.invoke(null);
		}
		catch (InvocationTargetException e) {
			ExceptionUtils.rethrowException(e.getTargetException(), "exception in method");
		}
	}

	@Test
	public void testCreateFsWithAuthority() throws Exception {
		final URI uri = URI.create("maprfs://localhost:12345/");

		MapRFsFactory factory = new MapRFsFactory();

		try {
			factory.create(uri);
			fail("should have failed with an exception");
		}
		catch (IOException e) {
			// expected, because we have no CLDB config available
		}
	}

	@Test
	public void testCreateFsWithMissingAuthority() throws Exception {
		final URI uri = URI.create("maprfs:///my/path");

		MapRFsFactory factory = new MapRFsFactory();
		factory.configure(new Configuration());

		FileSystem fs = factory.create(uri);
		assertEquals("maprfs", fs.getUri().getScheme());
	}

	// ------------------------------------------------------------------------

	private static final class MapRFreeClassLoader extends URLClassLoader {

		private final ClassLoader properParent;

		MapRFreeClassLoader(URLClassLoader parent) {
			super(parent.getURLs(), null);
			properParent = parent;
		}

		@Override
		public Class<?> loadClass(String name) throws ClassNotFoundException {
			if (name.startsWith("com.mapr") || name.startsWith("org.apache.hadoop")) {
				throw new ClassNotFoundException(name);
			}
			else if (name.startsWith("org.apache.log4j")) {
				return properParent.loadClass(name);
			}
			else {
				return super.loadClass(name);
			}
		}
	}
}
