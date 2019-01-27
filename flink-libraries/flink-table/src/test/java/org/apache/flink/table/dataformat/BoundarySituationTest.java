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

package org.apache.flink.table.dataformat;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.RandomAccessOutputView;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;
import org.apache.flink.table.typeutils.BinaryRowSerializer;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.dataformat.util.BinaryRowUtil.BYTE_ARRAY_BASE_OFFSET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Boundary test for binary methods.
 */
public class BoundarySituationTest {

	@Test
	public void testSerializerPages() throws IOException {
		BinaryRow row24 = get24BytesBinaryRow();
		BinaryRow row160 = get160BytesBinaryRow();
		testSerializerPagesInternal(row24, row160);
		testSerializerPagesInternal(row24, getVarSeg160BytesBinaryRow(row160));
	}

	private void testSerializerPagesInternal(BinaryRow row24, BinaryRow row160) throws IOException {
		BinaryRowSerializer serializer = new BinaryRowSerializer(DataTypes.STRING, DataTypes.STRING);

		// 1. test middle row with just on the edge1
		{
			MemorySegment[] segments = new MemorySegment[4];
			for (int i = 0; i < segments.length; i++) {
				segments[i] = MemorySegmentFactory.wrap(new byte[64]);
			}
			RandomAccessOutputView out = new RandomAccessOutputView(segments, segments[0].size());
			serializer.serializeToPages(row24, out);
			serializer.serializeToPages(row160, out);
			serializer.serializeToPages(row24, out);

			RandomAccessInputView in = new RandomAccessInputView(
					new ArrayList<>(Arrays.asList(segments)),
					segments[0].size(),
					out.getCurrentPositionInSegment());

			BinaryRow retRow = new BinaryRow(2);
			List<BinaryRow> rets = new ArrayList<>();
			while (true) {
				try {
					retRow = serializer.mapFromPages(retRow, in);
				} catch (EOFException e) {
					break;
				}
				rets.add(retRow.copy());
			}
			assertEquals(row24, rets.get(0));
			assertEquals(row160, rets.get(1));
			assertEquals(row24, rets.get(2));
		}

		// 2. test middle row with just on the edge2
		{
			MemorySegment[] segments = new MemorySegment[7];
			for (int i = 0; i < segments.length; i++) {
				segments[i] = MemorySegmentFactory.wrap(new byte[64]);
			}
			RandomAccessOutputView out = new RandomAccessOutputView(segments, segments[0].size());
			serializer.serializeToPages(row24, out);
			serializer.serializeToPages(row160, out);
			serializer.serializeToPages(row160, out);

			RandomAccessInputView in = new RandomAccessInputView(
					new ArrayList<>(Arrays.asList(segments)),
					segments[0].size(),
					out.getCurrentPositionInSegment());

			BinaryRow retRow = new BinaryRow(2);
			List<BinaryRow> rets = new ArrayList<>();
			while (true) {
				try {
					retRow = serializer.mapFromPages(retRow, in);
				} catch (EOFException e) {
					break;
				}
				rets.add(retRow.copy());
			}
			assertEquals(row24, rets.get(0));
			assertEquals(row160, rets.get(1));
			assertEquals(row160, rets.get(2));
		}

		// 3. test last row with just on the edge
		{
			MemorySegment[] segments = new MemorySegment[3];
			for (int i = 0; i < segments.length; i++) {
				segments[i] = MemorySegmentFactory.wrap(new byte[64]);
			}
			RandomAccessOutputView out = new RandomAccessOutputView(segments, segments[0].size());
			serializer.serializeToPages(row24, out);
			serializer.serializeToPages(row160, out);

			RandomAccessInputView in = new RandomAccessInputView(
					new ArrayList<>(Arrays.asList(segments)),
					segments[0].size(),
					out.getCurrentPositionInSegment());

			BinaryRow retRow = new BinaryRow(2);
			List<BinaryRow> rets = new ArrayList<>();
			while (true) {
				try {
					retRow = serializer.mapFromPages(retRow, in);
				} catch (EOFException e) {
					break;
				}
				rets.add(retRow.copy());
			}
			assertEquals(row24, rets.get(0));
			assertEquals(row160, rets.get(1));
		}
	}

	@Test
	public void testByteArrayEquals() {
		byte[] bytes1 = new byte[5];
		bytes1[3] = 81;
		byte[] bytes2 = new byte[100];
		bytes2[3] = 81;
		bytes2[4] = 81;

		assertTrue(BinaryRowUtil.byteArrayEquals(bytes1, bytes2, 4));
		assertFalse(BinaryRowUtil.byteArrayEquals(bytes1, bytes2, 5));
		assertTrue(BinaryRowUtil.byteArrayEquals(bytes1, bytes2, 0));
	}

	@Test
	public void testEquals() {
		BinaryRow row24 = get24BytesBinaryRow();
		BinaryRow row160 = get160BytesBinaryRow();
		BinaryRow varRow160 = getVarSeg160BytesBinaryRow(row160);
		BinaryRow varRow160InOne = getVarSeg160BytesInOneSegRow(row160);

		assertTrue(row160.equals(varRow160InOne));
		assertTrue(varRow160.equals(varRow160InOne));
		assertTrue(row160.equals(varRow160));
		assertTrue(varRow160InOne.equals(varRow160));

		assertFalse(row24.equals(row160));
		assertFalse(row24.equals(varRow160));
		assertFalse(row24.equals(varRow160InOne));

		assertTrue(BinaryRowUtil.equals(row24.getAllSegments(), 0, row160.getAllSegments(), 0, 0));
		assertTrue(BinaryRowUtil.equals(row24.getAllSegments(), 0, varRow160.getAllSegments(), 0, 0));
		assertTrue(BinaryRowUtil.equalsSlow(row24.getAllSegments(), 0, varRow160.getAllSegments(), 0, 0));

		// test var segs
		MemorySegment[] segments1 = new MemorySegment[2];
		segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
		segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
		MemorySegment[] segments2 = new MemorySegment[3];
		segments2[0] = MemorySegmentFactory.wrap(new byte[16]);
		segments2[1] = MemorySegmentFactory.wrap(new byte[16]);
		segments2[2] = MemorySegmentFactory.wrap(new byte[16]);

		segments1[0].put(9, (byte) 1);
		assertFalse(BinaryRowUtil.equals(segments1, 0, segments2, 14, 14));
		segments2[1].put(7, (byte) 1);
		assertTrue(BinaryRowUtil.equals(segments1, 0, segments2, 14, 14));
		assertTrue(BinaryRowUtil.equals(segments1, 2, segments2, 16, 14));
		assertTrue(BinaryRowUtil.equals(segments1, 2, segments2, 16, 16));

		segments2[2].put(7, (byte) 1);
		assertTrue(BinaryRowUtil.equals(segments1, 2, segments2, 32, 14));
	}

	@Test
	public void testCopy() {
		MemorySegment[] segments1 = new MemorySegment[2];
		segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
		segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
		segments1[0].put(15, (byte) 5);
		segments1[1].put(15, (byte) 6);

		{
			byte[] bytes = new byte[64];
			MemorySegment[] segments2 = new MemorySegment[]{MemorySegmentFactory.wrap(bytes)};

			BinaryRowUtil.copy(segments1, 0, bytes, 0, 64);
			assertTrue(BinaryRowUtil.equals(segments1, 0, segments2, 0, 64));
		}

		{
			byte[] bytes = new byte[64];
			MemorySegment[] segments2 = new MemorySegment[]{MemorySegmentFactory.wrap(bytes)};

			BinaryRowUtil.copy(segments1, 32, bytes, 0, 14);
			assertTrue(BinaryRowUtil.equals(segments1, 32, segments2, 0, 14));
		}

		{
			byte[] bytes = new byte[64];
			MemorySegment[] segments2 = new MemorySegment[]{MemorySegmentFactory.wrap(bytes)};

			BinaryRowUtil.copy(segments1, 34, bytes, 0, 14);
			assertTrue(BinaryRowUtil.equals(segments1, 34, segments2, 0, 14));
		}
	}

	@Test
	public void testCopyUnsafe() {
		MemorySegment[] segments1 = new MemorySegment[2];
		segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
		segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
		segments1[0].put(15, (byte) 5);
		segments1[1].put(15, (byte) 6);

		{
			byte[] bytes = new byte[64];
			MemorySegment[] segments2 = new MemorySegment[]{MemorySegmentFactory.wrap(bytes)};

			BinaryRowUtil.copyToUnsafe(segments1, 0, bytes, BYTE_ARRAY_BASE_OFFSET, 64);
			assertTrue(BinaryRowUtil.equals(segments1, 0, segments2, 0, 64));
		}

		{
			byte[] bytes = new byte[64];
			MemorySegment[] segments2 = new MemorySegment[]{MemorySegmentFactory.wrap(bytes)};

			BinaryRowUtil.copyToUnsafe(segments1, 32, bytes, BYTE_ARRAY_BASE_OFFSET, 14);
			assertTrue(BinaryRowUtil.equals(segments1, 32, segments2, 0, 14));
		}

		{
			byte[] bytes = new byte[64];
			MemorySegment[] segments2 = new MemorySegment[]{MemorySegmentFactory.wrap(bytes)};

			BinaryRowUtil.copyToUnsafe(segments1, 34, bytes, BYTE_ARRAY_BASE_OFFSET, 14);
			assertTrue(BinaryRowUtil.equals(segments1, 34, segments2, 0, 14));
		}
	}

	@Test
	public void testCopyFromUnsafe() {
		byte[] bytes = new byte[64];
		MemorySegment[] segments2 = new MemorySegment[]{MemorySegmentFactory.wrap(bytes)};
		bytes[15] = (byte) 5;
		bytes[47] = (byte) 6;

		{
			MemorySegment[] segments1 = new MemorySegment[2];
			segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
			segments1[1] = MemorySegmentFactory.wrap(new byte[32]);

			BinaryRowUtil.copyFromUnsafe(segments1, 0, bytes, BYTE_ARRAY_BASE_OFFSET, 64);
			assertTrue(BinaryRowUtil.equals(segments1, 0, segments2, 0, 64));
		}

		{
			MemorySegment[] segments1 = new MemorySegment[2];
			segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
			segments1[1] = MemorySegmentFactory.wrap(new byte[32]);

			BinaryRowUtil.copyFromUnsafe(segments1, 32, bytes, BYTE_ARRAY_BASE_OFFSET, 14);
			assertTrue(BinaryRowUtil.equals(segments1, 32, segments2, 0, 14));
		}

		{
			MemorySegment[] segments1 = new MemorySegment[2];
			segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
			segments1[1] = MemorySegmentFactory.wrap(new byte[32]);

			BinaryRowUtil.copyFromUnsafe(segments1, 34, bytes, BYTE_ARRAY_BASE_OFFSET, 14);
			assertTrue(BinaryRowUtil.equals(segments1, 34, segments2, 0, 14));
		}
	}

	@Test
	public void testFind() {
		MemorySegment[] segments1 = new MemorySegment[2];
		segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
		segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
		MemorySegment[] segments2 = new MemorySegment[3];
		segments2[0] = MemorySegmentFactory.wrap(new byte[16]);
		segments2[1] = MemorySegmentFactory.wrap(new byte[16]);
		segments2[2] = MemorySegmentFactory.wrap(new byte[16]);

		assertEquals(34, BinaryRowUtil.find(segments1, 34, 0, segments2, 0, 0));
		assertEquals(-1, BinaryRowUtil.find(segments1, 34, 0, segments2, 0, 15));
	}

	private static BinaryRow get24BytesBinaryRow() {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, RandomStringUtils.randomNumeric(2));
		writer.writeString(1, RandomStringUtils.randomNumeric(2));
		writer.complete();
		return row;
	}

	private static BinaryRow get160BytesBinaryRow() {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, RandomStringUtils.randomNumeric(72));
		writer.writeString(1, RandomStringUtils.randomNumeric(64));
		writer.complete();
		return row;
	}

	private static BinaryRow getVarSeg160BytesBinaryRow(BinaryRow row160) {
		BinaryRow varSegRow160 = new BinaryRow(2);
		MemorySegment[] segments = new MemorySegment[6];
		int baseOffset = 8;
		int posInSeg = baseOffset;
		int remainSize = 160;
		for (int i = 0; i < segments.length; i++) {
			segments[i] = MemorySegmentFactory.wrap(new byte[32]);
			int copy = Math.min(32 - posInSeg, remainSize);
			row160.getMemorySegment().copyTo(160 - remainSize, segments[i], posInSeg, copy);
			remainSize -= copy;
			posInSeg = 0;
		}
		varSegRow160.pointTo(segments, baseOffset, 160);
		assertEquals(row160, varSegRow160);
		return varSegRow160;
	}

	private static BinaryRow getVarSeg160BytesInOneSegRow(BinaryRow row160) {
		MemorySegment[] segments = new MemorySegment[2];
		segments[0] = row160.getMemorySegment();
		segments[1] = MemorySegmentFactory.wrap(new byte[row160.getMemorySegment().size()]);
		row160.pointTo(segments, 0, row160.getSizeInBytes());
		return row160;
	}

}
