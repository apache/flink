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

package org.apache.flink.table.temptable.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;
import org.apache.flink.table.typeutils.BaseRowSerializer;

import java.io.IOException;
import java.util.Collection;

/**
 * A helper class for Table Service.
 */
public final class BytesUtil {

	private BytesUtil() {}

	public static int bytesToInt(byte[] bytes) {
		return bytesToInt(bytes, 0);
	}

	public static int bytesToInt(byte[] bytes, int offset) {
		int ans = 0;
		ans |= (bytes[offset] & 0xff) << 24;
		ans |= (bytes[offset + 1] & 0xff) << 16;
		ans |= (bytes[offset + 2] & 0xff) << 8;
		ans |= (bytes[offset + 3] & 0xff);
		return ans;
	}

	public static byte[] intToBytes(int x) {
		byte[] buffer = new byte[4];
		buffer[0] = (byte) ((x >> 24) & 0xff);
		buffer[1] = (byte) ((x >> 16) & 0xff);
		buffer[2] = (byte) ((x >> 8) & 0xff);
		buffer[3] = (byte) (x & 0xff);
		return buffer;
	}

	public static byte[] intsToBytes(Collection<Integer> ints) {
		if (ints == null || ints.isEmpty()) {
			return new byte[0];
		}
		byte[] buffer = new byte[Integer.BYTES * ints.size()];
		int index = 0;
		for (Integer x : ints) {
			byte[] bytes = intToBytes(x);
			System.arraycopy(bytes, 0, buffer, index * Integer.BYTES, Integer.BYTES);
		}
		return buffer;
	}

	public static byte[] serialize(BaseRow baseRow, BaseRowSerializer baseRowSerializer) {
		BinaryRow binaryRow;
		if (baseRow.getClass() == BinaryRow.class) {
			binaryRow = (BinaryRow) baseRow;
		} else {
			try {
				binaryRow = baseRowSerializer.baseRowToBinary(baseRow);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		byte[] rowBytes = BinaryRowUtil.copy(binaryRow.getAllSegments(), binaryRow.getBaseOffset(), binaryRow.getSizeInBytes());

		byte[] buffer = new byte[binaryRow.getSizeInBytes() + Integer.BYTES];

		byte[] intBytes = BytesUtil.intToBytes(binaryRow.getSizeInBytes());

		for (int i = 0; i < Integer.BYTES; i++) {
			buffer[i] = intBytes[i];
		}

		System.arraycopy(rowBytes, 0, buffer, Integer.BYTES, binaryRow.getSizeInBytes());

		return buffer;
	}

	public static BaseRow deSerialize(byte[] buffer, int offset, int sizeInBytes, BaseRowSerializer baseRowSerializer) {
		MemorySegment memorySegment = MemorySegmentFactory.wrap(buffer);

		BinaryRow row = new BinaryRow(baseRowSerializer.getNumFields());
		row.pointTo(memorySegment, offset, sizeInBytes);

		return row;
	}

	public static BaseRow deSerialize(byte[] buffer, int sizeInBytes, BaseRowSerializer baseRowSerializer) {
		return deSerialize(buffer, 0, sizeInBytes, baseRowSerializer);
	}
}
