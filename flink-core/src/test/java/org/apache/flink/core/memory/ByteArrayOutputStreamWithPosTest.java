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

import org.apache.flink.configuration.ConfigConstants;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;

public class ByteArrayOutputStreamWithPosTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	/**
	 * This tests setting position which is exactly the same with the buffer size.
	 */
	@Test
	public void testSetPositionWhenBufferIsFull() throws Exception {

		int initBufferSize = 32;

		ByteArrayOutputStreamWithPos stream = new ByteArrayOutputStreamWithPos(initBufferSize);

		stream.write(new byte[initBufferSize]);

		// check whether the buffer is filled fully
		Assert.assertEquals(initBufferSize, stream.getBuf().length);

		// check current position is the end of the buffer
		Assert.assertEquals(initBufferSize, stream.getPosition());

		stream.setPosition(initBufferSize);

		// confirm current position is at where we expect.
		Assert.assertEquals(initBufferSize, stream.getPosition());

	}

	/**
	 * This tests setting negative position
	 */
	@Test
	public void testSetNegativePosition() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Position out of bounds");

		int initBufferSize = 32;

		ByteArrayOutputStreamWithPos stream = new ByteArrayOutputStreamWithPos(initBufferSize);

		stream.write(new byte[initBufferSize]);

		stream.setPosition(-1);

		Assert.fail("Should not reach here !!!!");
	}

	/**
	 * This tests setting position larger than buffer size
	 */
	@Test
	public void testSetPositionLargerThanBufferSize() throws Exception {
		int initBufferSize = 32;

		ByteArrayOutputStreamWithPos stream = new ByteArrayOutputStreamWithPos(initBufferSize);

		stream.write(new byte[initBufferSize]);

		Assert.assertEquals(initBufferSize, stream.getBuf().length);

		stream.setPosition(initBufferSize + 1);

		Assert.assertEquals(initBufferSize * 2, stream.getBuf().length);

		Assert.assertEquals(initBufferSize + 1, stream.getPosition());
	}

	/**
	 * THis tests that toString returns a substring of the buffer with range(0, position)
	 */
	@Test
	public void testToString() throws IOException {
		ByteArrayOutputStreamWithPos stream = new ByteArrayOutputStreamWithPos();

		byte[] data = "1234567890".getBytes(ConfigConstants.DEFAULT_CHARSET);
		stream.write(data);
		Assert.assertArrayEquals(data, stream.toString().getBytes(ConfigConstants.DEFAULT_CHARSET));

		stream.setPosition(1);
		Assert.assertArrayEquals(Arrays.copyOf(data, 1), stream.toString().getBytes(ConfigConstants.DEFAULT_CHARSET));

		stream.setPosition(data.length + 1);
		Assert.assertArrayEquals(Arrays.copyOf(data, data.length + 1), stream.toString().getBytes(ConfigConstants.DEFAULT_CHARSET));

	}
}
