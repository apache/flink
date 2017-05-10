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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.Assert.fail;

/**
 * This test validates that the Kryo-based serializer handles classes with custom
 * class loaders correctly.
 */
public class KryoSerializerClassLoadingTest extends SerializerTestBase<Object> {

	/** Class loader for the object that is not in the test class path */
	private static final ClassLoader CLASS_LOADER =
			new URLClassLoader(new URL[0], KryoSerializerClassLoadingTest.class.getClassLoader());

	/** An object that is not in the test class path */
	private static final Serializable OBJECT_OUT_OF_CLASSPATH =
			CommonTestUtils.createObjectForClassNotInClassPath(CLASS_LOADER);

	// ------------------------------------------------------------------------

	private ClassLoader originalClassLoader;

	@Before
	public void setupClassLoader() {
		originalClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(CLASS_LOADER);
	}

	@After
	public void restoreOriginalClassLoader() {
		Thread.currentThread().setContextClassLoader(originalClassLoader);
	}

	// ------------------------------------------------------------------------

	@Test
	public void guardTestAssumptions() {
		try {
			Class.forName(OBJECT_OUT_OF_CLASSPATH.getClass().getName());
			fail("This test's assumptions are broken");
		}
		catch (ClassNotFoundException ignored) {
			// expected
		}
	}

	// ------------------------------------------------------------------------

	@Override
	protected TypeSerializer<Object> createSerializer() {
		return new KryoSerializer<>(Object.class, new ExecutionConfig());
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<Object> getTypeClass() {
		return Object.class;
	}

	@Override
	protected Object[] getTestData() {
		return new Object[] {
				new Integer(7),

				// an object whose class is not on the classpath
				OBJECT_OUT_OF_CLASSPATH,

				// an object whose class IS on the classpath with a nested object whose class
				// is NOT on the classpath
				new Tuple1<>(OBJECT_OUT_OF_CLASSPATH)
		};
	}

	@Override
	public void testInstantiate() {
		// this serializer does not support instantiation
	}
}
