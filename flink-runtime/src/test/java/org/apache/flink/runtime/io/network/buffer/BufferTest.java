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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class BufferTest {

	@Test
	public void testSetGetSize() {
		final MemorySegment segment = new MemorySegment(new byte[1024]);
		final BufferRecycler recycler = Mockito.mock(BufferRecycler.class);

		Buffer buffer = new Buffer(segment, recycler);
		Assert.assertEquals(segment.size(), buffer.getSize());

		buffer.setSize(segment.size() / 2);
		Assert.assertEquals(segment.size() / 2, buffer.getSize());

		try {
			buffer.setSize(-1);
			Assert.fail("Didn't throw expected exception");
		} catch (IllegalArgumentException e) {
			// OK => expected exception
		}

		try {
			buffer.setSize(segment.size() + 1);
			Assert.fail("Didn't throw expected exception");
		} catch (IllegalArgumentException e) {
			// OK => expected exception
		}
	}

	@Test
	public void testExceptionAfterRecycle() throws Throwable {
		final MemorySegment segment = new MemorySegment(new byte[1024]);
		final BufferRecycler recycler = Mockito.mock(BufferRecycler.class);

		final Buffer buffer = new Buffer(segment, recycler);

		buffer.recycle();

		// Verify that the buffer has been recycled
		Mockito.verify(recycler, Mockito.times(1)).recycle(Matchers.any(MemorySegment.class));

		final Buffer spyBuffer = Mockito.spy(buffer);

		// Check that every method throws the appropriate exception after the
		// buffer has been recycled.
		//
		// Note: We cannot directly work on the spied upon buffer to get the
		// declared methods as Mockito adds some of its own.
		for (final Method method : buffer.getClass().getDeclaredMethods()) {
			if (Modifier.isPublic(method.getModifiers()) && !method.getName().equals("toString")
					&& !method.getName().equals("isRecycled") && !method.getName().equals("isBuffer")) {
				// Get method of the spied buffer to allow argument matchers
				final Method spyMethod = spyBuffer.getClass().getDeclaredMethod(method.getName(), method.getParameterTypes());

				final Class<?>[] paramTypes = spyMethod.getParameterTypes();
				final Object[] params = new Object[paramTypes.length];

				for (int i = 0; i < params.length; i++) {
					params[i] = Matchers.any(paramTypes[i]);
				}

				try {
					spyMethod.invoke(spyBuffer, params);
					Assert.fail("Didn't throw expected exception for method: " + method.getName());
				} catch (InvocationTargetException e) {
					if (e.getTargetException() instanceof IllegalStateException) {
						// OK => expected exception
					}
					else {
						throw e.getTargetException();
					}
				}
			}
		}
	}
}
