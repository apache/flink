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
import org.apache.flink.util.ClassLoaderUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URLClassLoader;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A class with tests that require to be run in a MapR/Hadoop-free environment,
 * to test proper error handling when no Hadoop classes are available.
 *
 * <p>This class must be dynamically loaded in a MapR/Hadoop-free class loader.
 */
public class MapRNotInClassPathTest extends TestLogger {

	@Test
	public void testInstantiationWhenMapRClassesAreMissing() throws Exception {
		final String testClassName = "org.apache.flink.runtime.fs.maprfs.MapRNotInClassPathTest$TestRunner";
		final ClassLoader cl = new MapRFreeClassLoader(getClass().getClassLoader());

		final RunnableWithException testRunner = Class
			.forName(testClassName, false, cl)
			.asSubclass(RunnableWithException.class)
			.newInstance();

		testRunner.run();
	}

	// ------------------------------------------------------------------------

	/**
	 * The tests that need to run in a special classloader.
	 */
	@SuppressWarnings("unused")
	public static final class TestRunner implements RunnableWithException {

		@Override
		public void run() throws Exception {
			// make sure no MapR or Hadoop FS classes are in the classpath
			try {
				Class.forName("com.mapr.fs.MapRFileSystem");
				fail("Cannot run test when MapR classes are in the classpath");
			}
			catch (ClassNotFoundException ignored) {}

			try {
				Class.forName("org.apache.hadoop.fs.FileSystem");
				fail("Cannot run test when Hadoop classes are in the classpath");
			}
			catch (ClassNotFoundException ignored) {}

			try {
				Class.forName("org.apache.hadoop.conf.Configuration");
				fail("Cannot run test when Hadoop classes are in the classpath");
			}
			catch (ClassNotFoundException ignored) {}

			// this method should complete without a linkage error
			final MapRFsFactory factory = new MapRFsFactory();

			// this method should also complete without a linkage error
			factory.configure(new Configuration());

			try {
				factory.create(new URI("maprfs://somehost:9000/root/dir"));
				fail("This statement should fail with an exception");
			}
			catch (IOException e) {
				assertTrue(e.getMessage().contains("MapR"));
				assertTrue(e.getMessage().contains("classpath"));
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A special classloader that filters "org.apache.hadoop.*" and "com.mapr.*" classes.
	 */
	private static final class MapRFreeClassLoader extends URLClassLoader {

		private final ClassLoader properParent;

		MapRFreeClassLoader(ClassLoader parent) {
			super(ClassLoaderUtils.getClasspathURLs(), null);
			properParent = parent;
		}

		@Override
		public Class<?> loadClass(String name) throws ClassNotFoundException {
			if (name.startsWith("com.mapr") || name.startsWith("org.apache.hadoop")) {
				throw new ClassNotFoundException(name);
			}
			else if (name.equals(RunnableWithException.class.getName()) || name.startsWith("org.apache.log4j")) {
				return properParent.loadClass(name);
			}
			else {
				return super.loadClass(name);
			}
		}
	}
}
