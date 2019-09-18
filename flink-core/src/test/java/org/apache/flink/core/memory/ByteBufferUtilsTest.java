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

package org.apache.flink.core.memory;

import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests for {@link ByteBufferUtils}.
 */
public class ByteBufferUtilsTest extends TestLogger {
	private final byte[] leftBufferBytes = new byte[]{'a', 'b', 'c', 'd', 'e'};
	private final byte[] rightBufferBytes = new byte[]{'b', 'c', 'd', 'e', 'f'};

	@Test
	public void testDirectBBWriteAndRead() {
		testWithDifferentOffset(true);
	}

	@Test
	public void testHeapBBWriteAndRead() {
		testWithDifferentOffset(false);
	}

	@Test
	public void testCompareDirectBBToArray() {
		testCompareTo(true, false, false);
	}

	@Test
	public void testCompareDirectBBToDirectBB() {
		testCompareTo(true, true, true);
	}

	@Test
	public void testCompareDirectBBToHeapBB() {
		testCompareTo(true, true, false);
	}

	@Test
	public void testCompareHeapBBToArray() {
		testCompareTo(false, false, false);
	}

	@Test
	public void testCompareHeapBBToDirectBB() {
		testCompareTo(false, true, true);
	}

	@Test
	public void testCompareHeapBBToHeapBB() {
		testCompareTo(false, true, false);
	}

	private void testCompareTo(boolean isLeftBBDirect, boolean isRightBuffer, boolean isRightDirect) {
		ByteBuffer left = getByteBuffer(isLeftBBDirect, leftBufferBytes);
		if (isRightBuffer) {
			ByteBuffer right = getByteBuffer(isRightDirect, rightBufferBytes);
			Assert.assertThat(ByteBufferUtils.compareTo(left, 1, 4, right, 0, 4),
				is(0));
			Assert.assertThat(ByteBufferUtils.compareTo(left, 1, 4, right, 1, 4),
				lessThan(0));
			Assert.assertThat(ByteBufferUtils.compareTo(left, 1, 4, right, 0, 3),
				greaterThan(0));
		} else {
			Assert.assertThat(ByteBufferUtils.compareTo(left, 1, 4, rightBufferBytes, 0, 4),
				is(0));
			Assert.assertThat(ByteBufferUtils.compareTo(left, 1, 4, rightBufferBytes, 1, 4),
				lessThan(0));
			Assert.assertThat(ByteBufferUtils.compareTo(left, 1, 4, rightBufferBytes, 0, 3),
				greaterThan(0));
		}
	}

	private ByteBuffer getByteBuffer(boolean isBBDirect, byte[] value) {
		return isBBDirect ? ByteBuffer.allocateDirect(value.length).put(value) : ByteBuffer.wrap(value);
	}

	private void testWithDifferentOffset(boolean direct) {
		int bufferSize = 4096;
		int offsetNumber = 7;
		ByteBuffer bb = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
		for (int offset = 0; offset < offsetNumber; offset++) {
			doCompositeTest(bb, offset);
		}
	}

	private void doCompositeTest(ByteBuffer bb, int offset) {
		int positionOri = bb.position();

		// put an int into the buffer at the given offset, confirm it could be read and buffer position won't change
		ByteBufferUtils.putInt(bb, offset, 123);
		Assert.assertEquals(bb.position(), positionOri);
		Assert.assertEquals(123, ByteBufferUtils.toInt(bb, offset));
		Assert.assertEquals(bb.position(), positionOri);

		// put a long next to the first int (4 bytes), confirm it could be read and buffer position won't change
		ByteBufferUtils.putLong(bb, offset + 4, 1234);
		Assert.assertEquals(bb.position(), positionOri);
		Assert.assertEquals(1234, ByteBufferUtils.toLong(bb, offset + 4));
		Assert.assertEquals(bb.position(), positionOri);

		// check and confirm the first int could be read correctly and buffer position won't change
		Assert.assertEquals(123, ByteBufferUtils.toInt(bb, offset));
		Assert.assertEquals(bb.position(), positionOri);

		// copy data into a new buffer
		ByteBuffer bb2 = ByteBuffer.allocate(12);
		int positionOri2 = bb2.position();
		ByteBufferUtils.copyFromBufferToBuffer(bb, offset, bb2, 0, 12);

		// check and confirm the int value is correctly copied
		Assert.assertEquals(ByteBufferUtils.toInt(bb2, 0), 123);
		// check and confirm the long value is correctly copied
		Assert.assertEquals(ByteBufferUtils.toLong(bb2, 4), 1234);
		// check and confirm the buffers' position won't change after copy operation
		Assert.assertEquals(bb.position(), positionOri);
		Assert.assertEquals(bb2.position(), positionOri2);
	}
}
