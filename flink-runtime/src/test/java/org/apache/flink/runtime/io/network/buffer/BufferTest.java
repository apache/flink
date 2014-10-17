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
import java.util.Random;

public class BufferTest {

	@Test
	public void testSetGetSize() {
		final MemorySegment segment = new MemorySegment(new byte[1024]);
		final BufferRecycler recycler = Mockito.mock(BufferRecycler.class);

		try {
			new Buffer(segment, -1, recycler);
			Assert.fail("Didn't throw expected exception");
		} catch (IllegalArgumentException e) {
			// OK => expected exception
		}

		try {
			new Buffer(segment, segment.size() + 1, recycler);
			Assert.fail("Didn't throw expected exception");
		} catch (IllegalArgumentException e) {
			// OK => expected exception
		}

		Buffer buffer = new Buffer(segment, segment.size(), recycler);
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

		final Buffer buffer = new Buffer(segment, segment.size(), recycler);

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
			if (Modifier.isPublic(method.getModifiers()) && !method.getName().equals("toString")) {
				// Get method of the spied buffer to allow argument matchers
				final Method spyMethod = spyBuffer.getClass().getDeclaredMethod(method.getName(), method.getParameterTypes());

				final Class<?>[] paramTypes = spyMethod.getParameterTypes();
				final Object[] params = new Object[paramTypes.length];

				for (int i = 0; i < params.length; i++) {
					params[i] = Matchers.any(paramTypes[i]);
				}

				try {
					spyMethod.invoke(spyBuffer, params);
					Assert.fail("Didn't throw expected exception");
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

	@Test
	public void testCopyTo() {
		final BufferRecycler recycler = Mockito.mock(BufferRecycler.class);
		final Random random = new Random();

		final byte[] sourceData = new byte[1024];
		final byte[] targetData = new byte[1024];
		final byte[] smallTargetData = new byte[512];

		final Buffer sourceBuffer = new Buffer(new MemorySegment(sourceData), sourceData.length, recycler);
		final Buffer targetBuffer = new Buffer(new MemorySegment(targetData), targetData.length, recycler);
		final Buffer smallTargetBuffer = new Buffer(new MemorySegment(smallTargetData), smallTargetData.length, recycler);

		// --------------------------------------------------------------------
		// Copy to buffer of same size (both Buffer and backing MemorySegment)
		// --------------------------------------------------------------------
		random.nextBytes(sourceData);
		random.nextBytes(targetData);

		sourceBuffer.copyTo(targetBuffer);

		Assert.assertEquals(sourceBuffer.getSize(), targetBuffer.getSize());
		for (int i = 0; i < sourceBuffer.getSize(); i++) {
			Assert.assertEquals(sourceBuffer.getMemorySegment().get(i), targetBuffer.getMemorySegment().get(i));
		}

		// --------------------------------------------------------------------
		// Copy to buffer of different size (same size backing MemorySegment)
		// --------------------------------------------------------------------
		random.nextBytes(sourceData);
		random.nextBytes(targetData);

		targetBuffer.setSize(targetBuffer.getSize() / 2);

		// Same size for backing MemorySegment, but not Buffer
		sourceBuffer.copyTo(targetBuffer);

		Assert.assertEquals(sourceBuffer.getSize(), targetBuffer.getSize());
		for (int i = 0; i < sourceBuffer.getSize(); i++) {
			Assert.assertEquals(sourceBuffer.getMemorySegment().get(i), targetBuffer.getMemorySegment().get(i));
		}

		// --------------------------------------------------------------------
		// Copy to buffer with smaller backing MemorySegment
		// --------------------------------------------------------------------

		// Target MemorySegment large enough for source buffer
		sourceBuffer.setSize(smallTargetBuffer.getSize());

		random.nextBytes(sourceData);
		random.nextBytes(smallTargetData);

		sourceBuffer.copyTo(smallTargetBuffer);

		Assert.assertEquals(sourceBuffer.getSize(), smallTargetBuffer.getSize());
		for (int i = 0; i < sourceBuffer.getSize(); i++) {
			Assert.assertEquals(sourceBuffer.getMemorySegment().get(i), smallTargetBuffer.getMemorySegment().get(i));
		}

		// Target MemorySegment too small for source buffer
		sourceBuffer.setSize(sourceData.length);

		random.nextBytes(sourceData);
		random.nextBytes(smallTargetData);

		try {
			targetBuffer.copyTo(smallTargetBuffer);
			Assert.fail("Didn't throw expected exception");
		} catch (IllegalArgumentException e) {
			// OK => expected exception
		}
	}
}
