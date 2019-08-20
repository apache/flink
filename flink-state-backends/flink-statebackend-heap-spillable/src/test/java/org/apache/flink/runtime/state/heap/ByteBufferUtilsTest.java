/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Tests for {@link ByteBufferUtils}.
 */
public class ByteBufferUtilsTest {

	@Test
	public void testBBWriteAndRead() {

		ByteBuffer bb = ByteBuffer.allocateDirect(4096);
		doTest(bb, 0);
		doTest(bb, 1);
		doTest(bb, 2);
		doTest(bb, 3);
		doTest(bb, 4);
		doTest(bb, 5);
		doTest(bb, 6);
		doTest(bb, 7);

		bb = ByteBuffer.allocate(4096);
		doTest(bb, 0);
		doTest(bb, 1);
		doTest(bb, 2);
		doTest(bb, 3);
		doTest(bb, 4);
		doTest(bb, 5);
		doTest(bb, 6);
		doTest(bb, 7);
	}

	private void doTest(ByteBuffer bb, int offset) {
		int positionOri = bb.position();

		ByteBufferUtils.putInt(bb, offset, 123);
		Assert.assertEquals(bb.position(), positionOri);
		Assert.assertEquals(123, ByteBufferUtils.toInt(bb, offset));
		Assert.assertEquals(bb.position(), positionOri);

		ByteBufferUtils.putLong(bb, offset + 4, 1234);
		Assert.assertEquals(bb.position(), positionOri);
		Assert.assertEquals(1234, ByteBufferUtils.toLong(bb, offset + 4));
		Assert.assertEquals(bb.position(), positionOri);

		Assert.assertEquals(123, ByteBufferUtils.toInt(bb, offset));
		Assert.assertEquals(bb.position(), positionOri);

		ByteBuffer bb2 = ByteBuffer.allocate(12);
		int positionOri2 = bb2.position();
		ByteBufferUtils.copyFromBufferToBuffer(bb, bb2, offset, 0, 12);

		Assert.assertEquals(ByteBufferUtils.toInt(bb2, 0), 123);
		Assert.assertEquals(ByteBufferUtils.toLong(bb2, 4), 1234);
		Assert.assertEquals(bb.position(), positionOri);
		Assert.assertEquals(bb2.position(), positionOri2);
	}
}
