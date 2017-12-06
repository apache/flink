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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.core.fs.FSDataInputStream;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link ByteStreamStateHandle}.
 */
public class ByteStreamStateHandleTest {

	@Test
	public void testStreamSeekAndPos() throws IOException {
		final byte[] data = {34, 25, 22, 66, 88, 54};
		final ByteStreamStateHandle handle = new ByteStreamStateHandle("name", data);

		// read backwards, one byte at a time
		for (int i = data.length; i >= 0; i--) {
			FSDataInputStream in = handle.openInputStream();
			in.seek(i);

			assertEquals(i, (int) in.getPos());

			if (i < data.length) {
				assertEquals((int) data[i], in.read());
				assertEquals(i + 1, (int) in.getPos());
			} else {
				assertEquals(-1, in.read());
				assertEquals(i, (int) in.getPos());
			}
		}

		// reading past the end makes no difference
		FSDataInputStream in = handle.openInputStream();
		in.seek(data.length);

		// read multiple times, should not affect anything
		assertEquals(-1, in.read());
		assertEquals(-1, in.read());
		assertEquals(-1, in.read());

		assertEquals(data.length, (int) in.getPos());
	}

	@Test
	public void testStreamSeekOutOfBounds() throws IOException {
		final int len = 10;
		final ByteStreamStateHandle handle = new ByteStreamStateHandle("name", new byte[len]);

		// check negative offset
		FSDataInputStream in = handle.openInputStream();
		try {
			in.seek(-2);
			fail("should fail with an exception");
		} catch (IOException e) {
			// expected
		}

		// check integer overflow
		in = handle.openInputStream();
		try {
			in.seek(len + 1);
			fail("should fail with an exception");
		} catch (IOException e) {
			// expected
		}

		// check integer overflow
		in = handle.openInputStream();
		try {
			in.seek(((long) Integer.MAX_VALUE) + 100L);
			fail("should fail with an exception");
		} catch (IOException e) {
			// expected
		}
	}

	@Test
	public void testBulkRead() throws IOException {
		final byte[] data = {34, 25, 22, 66};
		final ByteStreamStateHandle handle = new ByteStreamStateHandle("name", data);
		final int targetLen = 8;

		for (int start = 0; start < data.length; start++) {
			for (int num = 0; num < targetLen; num++) {
				FSDataInputStream in = handle.openInputStream();
				in.seek(start);

				final byte[] target = new byte[targetLen];
				final int read = in.read(target, targetLen - num, num);

				assertEquals(Math.min(num, data.length - start), read);
				for (int i = 0; i < read; i++) {
					assertEquals(data[start + i], target[targetLen - num + i]);
				}

				int newPos = start + read;
				assertEquals(newPos, (int) in.getPos());
				assertEquals(newPos < data.length ? data[newPos] : -1, in.read());
			}
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void testBulkReadINdexOutOfBounds() throws IOException {
		final ByteStreamStateHandle handle = new ByteStreamStateHandle("name", new byte[10]);

		// check negative offset
		FSDataInputStream in = handle.openInputStream();
		try {
			in.read(new byte[10], -1, 5);
			fail("should fail with an exception");
		} catch (IndexOutOfBoundsException e) {
			// expected
		}

		// check offset overflow
		in = handle.openInputStream();
		try {
			in.read(new byte[10], 10, 5);
			fail("should fail with an exception");
		} catch (IndexOutOfBoundsException e) {
			// expected
		}

		// check negative length
		in = handle.openInputStream();
		try {
			in.read(new byte[10], 0, -2);
			fail("should fail with an exception");
		} catch (IndexOutOfBoundsException e) {
			// expected
		}

		// check length too large
		in = handle.openInputStream();
		try {
			in.read(new byte[10], 5, 6);
			fail("should fail with an exception");
		} catch (IndexOutOfBoundsException e) {
			// expected
		}

		// check length integer overflow
		in = handle.openInputStream();
		try {
			in.read(new byte[10], 5, Integer.MAX_VALUE);
			fail("should fail with an exception");
		} catch (IndexOutOfBoundsException e) {
			// expected
		}
	}
}
