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
import org.junit.Test;

public class ByteArrayInputStreamWithPosTest {

	/**
	 *  This tests setting position on a {@link ByteArrayInputStreamWithPos}
	 */
	@Test
	public void testSetPosition() throws Exception {
		byte[] data = new byte[] {'0','1','2','3','4','5','6','7','8','9'};
		ByteArrayInputStreamWithPos inputStreamWithPos = new ByteArrayInputStreamWithPos(data);
		inputStreamWithPos.setPosition(1);
		Assert.assertEquals('1', inputStreamWithPos.read());
		inputStreamWithPos.setPosition(3);
		Assert.assertEquals('3', inputStreamWithPos.read());
	}

	/**
	 * This tests that the expected position exceeds the capacity of the byte array.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testSetErrorPosition() throws Exception {
		byte[] data = new byte[] {'0','1','2','3','4','5','6','7','8','9'};
		ByteArrayInputStreamWithPos inputStreamWithPos = new ByteArrayInputStreamWithPos(data);
		inputStreamWithPos.setPosition(data.length);
		Assert.fail("Should not reach here !!!!");
	}

}
