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

package org.apache.flink.core.memory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link ByteArrayInputStreamWithPos}.
 */
public class ByteArrayInputStreamWithPosTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private byte[] data = new byte[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

	private ByteArrayInputStreamWithPos stream;

	@Before
	public void setup() {
		stream = new ByteArrayInputStreamWithPos(data);
	}

	/**
	 * Test setting position on a {@link ByteArrayInputStreamWithPos}.
	 */
	@Test
	public void testSetPosition() throws Exception {
		Assert.assertEquals(data.length, stream.available());
		Assert.assertEquals('0', stream.read());

		stream.setPosition(1);
		Assert.assertEquals(data.length - 1, stream.available());
		Assert.assertEquals('1', stream.read());

		stream.setPosition(3);
		Assert.assertEquals(data.length - 3, stream.available());
		Assert.assertEquals('3', stream.read());

		stream.setPosition(data.length);
		Assert.assertEquals(0, stream.available());
		Assert.assertEquals(-1, stream.read());
	}

	/**
	 * Test that the expected position exceeds the capacity of the byte array.
	 */
	@Test
	public void testSetTooLargePosition() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Position out of bounds.");
		stream.setPosition(data.length + 1);
	}

	/**
	 * Test setting a negative position.
	 */
	@Test
	public void testSetNegativePosition() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Position out of bounds.");
		stream.setPosition(-1);
	}

	@Test
	public void testSetBuffer() {
		ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos();
		Assert.assertEquals(-1, in.read());
		byte[] testData = new byte[]{0x42, 0x43, 0x44, 0x45};
		int off = 1;
		int len = 2;
		in.setBuffer(testData, off, len);
		for (int i = 0; i < len; ++i) {
			Assert.assertEquals(testData[i + off], in.read());
		}
		Assert.assertEquals(-1, in.read());
	}
}
