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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.util.ClassLoaderUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Tests that validate the behavior of the Hadoop File System Factory.
 */
public class HadoopFreeFsFactoryTest extends TestLogger {

	/**
	 * This test validates that the factory can be instantiated and configured even
	 * when Hadoop classes are missing from the classpath.
	 */
	@Test
	public void testHadoopFactoryInstantiationWithoutHadoop() throws Exception {
		// we do reflection magic here to instantiate the test in another class
		// loader, to make sure no hadoop classes are in the classpath

		final String testClassName = "org.apache.flink.runtime.fs.hdfs.HadoopFreeTests";

		final URL[] urls = ClassLoaderUtils.getClasspathURLs();

		ClassLoader parent = getClass().getClassLoader();
		ClassLoader hadoopFreeClassLoader = new HadoopFreeClassLoader(urls, parent);
		Class<?> testClass = Class.forName(testClassName, false, hadoopFreeClassLoader);
		Method m = testClass.getDeclaredMethod("test");

		try {
			m.invoke(null);
		}
		catch (InvocationTargetException e) {
			ExceptionUtils.rethrowException(e.getTargetException(), "exception in method");
		}
	}

	// ------------------------------------------------------------------------

	private static final class HadoopFreeClassLoader extends URLClassLoader {

		private final ClassLoader properParent;

		HadoopFreeClassLoader(URL[] urls, ClassLoader parent) {
			super(urls, null);
			properParent = parent;
		}

		@Override
		public Class<?> loadClass(String name) throws ClassNotFoundException {
			if (name.startsWith("org.apache.hadoop")) {
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
