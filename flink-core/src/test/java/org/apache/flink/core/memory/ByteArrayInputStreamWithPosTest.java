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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ByteArrayInputStreamWithPosTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	/**
	 *  This tests setting position on a {@link ByteArrayInputStreamWithPos}
	 */
	@Test
	public void testSetPosition() throws Exception {
		byte[] data = new byte[] {'0','1','2','3','4','5','6','7','8','9'};
		ByteArrayInputStreamWithPos inputStreamWithPos = new ByteArrayInputStreamWithPos(data);
		inputStreamWithPos.setPosition(1);
		Assert.assertEquals(data.length - 1, inputStreamWithPos.available());
		Assert.assertEquals('1', inputStreamWithPos.read());
		inputStreamWithPos.setPosition(3);
		Assert.assertEquals(data.length - 3, inputStreamWithPos.available());
		Assert.assertEquals('3', inputStreamWithPos.read());
		inputStreamWithPos.setPosition(data.length);
		Assert.assertEquals(0, inputStreamWithPos.available());
		Assert.assertEquals(-1, inputStreamWithPos.read());
	}

	/**
	 * This tests that the expected position exceeds the capacity of the byte array.
	 */
	@Test
	public void testSetTooLargePosition() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Position out of bounds.");
		byte[] data = new byte[] {'0','1','2','3','4','5','6','7','8','9'};
		ByteArrayInputStreamWithPos inputStreamWithPos = new ByteArrayInputStreamWithPos(data);
		inputStreamWithPos.setPosition(data.length + 1);
		Assert.fail("Should not reach here !!!!");
	}

	/**
	 * This tests setting a negative position
	 */
	@Test
	public void testSetNegativePosition() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Position out of bounds.");
		byte[] data = new byte[] {'0','1','2','3','4','5','6','7','8','9'};
		ByteArrayInputStreamWithPos inputStreamWithPos = new ByteArrayInputStreamWithPos(data);
		inputStreamWithPos.setPosition(-1);
		Assert.fail("Should not reach here !!!!");
	}

}
