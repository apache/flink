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

package org.apache.flink.runtime.util;

import org.apache.flink.core.memory.MemoryUtils;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedThrowable;

import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSource;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;

import static org.junit.Assert.*;

public class SerializedThrowableTest {
	
	@Test
	public void testIdenticalMessageAndStack() {
		try {
			IllegalArgumentException original = new IllegalArgumentException("test message");
			SerializedThrowable serialized = new SerializedThrowable(original);
			
			assertEquals(original.getMessage(), serialized.getMessage());
			assertEquals(original.toString(), serialized.toString());
			
			assertEquals(ExceptionUtils.stringifyException(original),
							ExceptionUtils.stringifyException(serialized));
			
			assertArrayEquals(original.getStackTrace(), serialized.getStackTrace());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSerialization() {
		try {
			// We need an exception whose class is not in the core class loader
			// we solve that by defining an exception class dynamically
			
			// an exception class, as bytes 
			final byte[] classData = {
					-54, -2, -70, -66, 0, 0, 0, 51, 0, 21, 10, 0, 3, 0, 18, 7, 0, 19, 7, 0, 20, 1,
					0, 16, 115, 101, 114, 105, 97, 108, 86, 101, 114, 115, 105, 111, 110, 85, 73,
					68, 1, 0, 1, 74, 1, 0, 13, 67, 111, 110, 115, 116, 97, 110, 116, 86, 97, 108,
					117, 101, 5, -103, -52, 22, -41, -23, -36, -25, 47, 1, 0, 6, 60, 105, 110, 105,
					116, 62, 1, 0, 3, 40, 41, 86, 1, 0, 4, 67, 111, 100, 101, 1, 0, 15, 76, 105,
					110, 101, 78, 117, 109, 98, 101, 114, 84, 97, 98, 108, 101, 1, 0, 18, 76, 111,
					99, 97, 108, 86, 97, 114, 105, 97, 98, 108, 101, 84, 97, 98, 108, 101, 1, 0, 4,
					116, 104, 105, 115, 1, 0, 61, 76, 111, 114, 103, 47, 97, 112, 97, 99, 104, 101,
					47, 102, 108, 105, 110, 107, 47, 114, 117, 110, 116, 105, 109, 101, 47, 117,
					116, 105, 108, 47, 84, 101, 115, 116, 69, 120, 99, 101, 112, 116, 105, 111,
					110, 70, 111, 114, 83, 101, 114, 105, 97, 108, 105, 122, 97, 116, 105, 111,
					110, 59, 1, 0, 10, 83, 111, 117, 114, 99, 101, 70, 105, 108, 101, 1, 0, 34, 84,
					101, 115, 116, 69, 120, 99, 101, 112, 116, 105, 111, 110, 70, 111, 114, 83,
					101, 114, 105, 97, 108, 105, 122, 97, 116, 105, 111, 110, 46, 106, 97, 118, 97,
					12, 0, 9, 0, 10, 1, 0, 59, 111, 114, 103, 47, 97, 112, 97, 99, 104, 101, 47,
					102, 108, 105, 110, 107, 47, 114, 117, 110, 116, 105, 109, 101, 47, 117, 116,
					105, 108, 47, 84, 101, 115, 116, 69, 120, 99, 101, 112, 116, 105, 111, 110, 70,
					111, 114, 83, 101, 114, 105, 97, 108, 105, 122, 97, 116, 105, 111, 110, 1, 0,
					19, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 69, 120, 99, 101, 112, 116,
					105, 111, 110, 0, 33, 0, 2, 0, 3, 0, 0, 0, 1, 0, 26, 0, 4, 0, 5, 0, 1, 0, 6, 0,
					0, 0, 2, 0, 7, 0, 1, 0, 1, 0, 9, 0, 10, 0, 1, 0, 11, 0, 0, 0, 47, 0, 1, 0, 1, 0,
					0, 0, 5, 42, -73, 0, 1, -79, 0, 0, 0, 2, 0, 12, 0, 0, 0, 6, 0, 1, 0, 0, 0, 21,
					0, 13, 0, 0, 0, 12, 0, 1, 0, 0, 0, 5, 0, 14, 0, 15, 0, 0, 0, 1, 0, 16, 0, 0, 0,
					2, 0, 17};

			// dummy class loader that has no access to any classes
			ClassLoader loader = new URLClassLoader(new URL[0]);

			// define a class into the classloader
			Class<?> clazz = MemoryUtils.UNSAFE.defineClass(
					"org.apache.flink.runtime.util.TestExceptionForSerialization",
					classData, 0, classData.length,
					loader,
					new ProtectionDomain(new CodeSource(null, (Certificate[]) null), new Permissions()));
			
			// create an instance of the exception (no message, no cause)
			Exception userException = clazz.asSubclass(Exception.class).newInstance();
			
			// check that we cannot simply copy the exception
			try {
				byte[] serialized = InstantiationUtil.serializeObject(userException);
				InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
				fail("should fail with a class not found exception");
			}
			catch (ClassNotFoundException e) {
				// as we want it
			}
			
			// validate that the SerializedThrowable mimics the original exception
			SerializedThrowable serialized = new SerializedThrowable(userException);
			assertEquals(userException.getMessage(), serialized.getMessage());
			assertEquals(userException.toString(), serialized.toString());
			assertEquals(ExceptionUtils.stringifyException(userException),
					ExceptionUtils.stringifyException(serialized));
			assertArrayEquals(userException.getStackTrace(), serialized.getStackTrace());

			// copy the serialized throwable and make sure everything still works
			SerializedThrowable copy = CommonTestUtils.createCopySerializable(serialized);
			assertEquals(userException.getMessage(), copy.getMessage());
			assertEquals(userException.toString(), copy.toString());
			assertEquals(ExceptionUtils.stringifyException(userException),
					ExceptionUtils.stringifyException(copy));
			assertArrayEquals(userException.getStackTrace(), copy.getStackTrace());
			
			// deserialize the proper exception
			Throwable deserialized = copy.deserializeError(loader); 
			assertEquals(clazz, deserialized.getClass());

			// deserialization with the wrong classloader does not lead to a failure
			Throwable wronglyDeserialized = copy.deserializeError(getClass().getClassLoader());
			assertEquals(ExceptionUtils.stringifyException(userException),
					ExceptionUtils.stringifyException(wronglyDeserialized));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCauseChaining() {
		Exception cause2 = new Exception("level2");
		Exception cause1 = new Exception("level1", cause2);
		Exception root = new Exception("level0", cause1);

		SerializedThrowable st = new SerializedThrowable(root);

		assertEquals("level0", st.getMessage());

		assertNotNull(st.getCause());
		assertEquals("level1", st.getCause().getMessage());

		assertNotNull(st.getCause().getCause());
		assertEquals("level2", st.getCause().getCause().getMessage());
	}

	@Test
	public void testCyclicCauseChaining() {
		Exception cause3 = new Exception("level3");
		Exception cause2 = new Exception("level2", cause3);
		Exception cause1 = new Exception("level1", cause2);
		Exception root = new Exception("level0", cause1);

		// introduce a cyclic reference
		cause3.initCause(cause1);

		SerializedThrowable st = new SerializedThrowable(root);

		assertArrayEquals(root.getStackTrace(), st.getStackTrace());
		assertEquals(ExceptionUtils.stringifyException(root), ExceptionUtils.stringifyException(st));
	}

	@Test
	public void testCopyPreservesCause() {
		Exception original = new Exception("original message");
		Exception parent = new Exception("parent message", original);

		SerializedThrowable serialized = new SerializedThrowable(parent);
		assertNotNull(serialized.getCause());

		SerializedThrowable copy = new SerializedThrowable(serialized);
		assertEquals("parent message", copy.getMessage());
		assertNotNull(copy.getCause());
		assertEquals("original message", copy.getCause().getMessage());
	}
}
