/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sources.parquet;

import org.apache.flink.core.memory.MemoryUtils;
import org.apache.flink.table.dataformat.vector.BooleanColumnVector;
import org.apache.flink.table.dataformat.vector.ByteColumnVector;
import org.apache.flink.table.dataformat.vector.BytesColumnVector;
import org.apache.flink.table.dataformat.vector.DoubleColumnVector;
import org.apache.flink.table.dataformat.vector.FloatColumnVector;
import org.apache.flink.table.dataformat.vector.IntegerColumnVector;
import org.apache.flink.table.dataformat.vector.LongColumnVector;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * An implementation of the Parquet PLAIN decoder that supports the vectorized interface.
 */
public class VectorizedPlainValuesReader extends ValuesReader implements VectorizedValuesReader {
	private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;
	private static final int BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
	private static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();
	private static final boolean BIG_ENDIAN_PLATFORM = NATIVE_BYTE_ORDER.equals(ByteOrder.BIG_ENDIAN);
	private static final int DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);
	private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;
	private byte[] buffer;
	private int offset;
	private int bitOffset; // Only used for booleans.
	private ByteBuffer byteBuffer; // used to wrap the byte array buffer

	@Override
	public void initFromPage(int valueCount, byte[] bytes, int offset) throws IOException {
		this.buffer = bytes;
		this.offset = offset + BYTE_ARRAY_OFFSET;
		if (BIG_ENDIAN_PLATFORM) {
			byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		}
	}

	@Override
	public void skip() {
		throw new UnsupportedOperationException();
	}

	@Override
	public final void readIntegers(int total, IntegerColumnVector c, int rowId) {
		for (int i = 0; i < total; ++i, offset += 4) {
			c.vector[i + rowId] = UNSAFE.getInt(buffer, (long) offset);
			if (BIG_ENDIAN_PLATFORM) {
				c.vector[i + rowId] = java.lang.Integer.reverseBytes(c.vector[i + rowId]);
			}
		}
	}

	@Override
	public void readLongs(int total, LongColumnVector c, int rowId) {
		for (int i = 0; i < total; ++i, offset += 8) {
			c.vector[i + rowId] = UNSAFE.getLong(buffer, (long) offset);
			if (BIG_ENDIAN_PLATFORM) {
				c.vector[i + rowId] = java.lang.Long.reverseBytes(c.vector[i + rowId]);
			}
		}
	}

	@Override
	public final long readLong() {
		long v = UNSAFE.getLong(buffer, (long) offset);
		if (BIG_ENDIAN_PLATFORM) {
			v = java.lang.Long.reverseBytes(v);
		}
		offset += 8;
		return v;
	}

	@Override
	public void readFloats(int total, FloatColumnVector c, int rowId) {
		if (!BIG_ENDIAN_PLATFORM) {
			copyMemory(buffer, offset, c.vector,
					DOUBLE_ARRAY_OFFSET + rowId * 4, total * 4);
		} else {
			ByteBuffer bb = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
			for (int i = 0; i < total; ++i) {
				c.vector[i + rowId] = bb.getFloat(offset - BYTE_ARRAY_OFFSET + (4 * i));
			}
		}
		offset += 4 * total;
	}

	@Override
	public final float readFloat() {
		float v;
		if (!BIG_ENDIAN_PLATFORM) {
			v = UNSAFE.getFloat(buffer, (long) offset);
		} else {
			v = byteBuffer.getFloat(offset - BYTE_ARRAY_OFFSET);
		}
		offset += 4;
		return v;
	}

	@Override
	public void readDoubles(int total, DoubleColumnVector c, int rowId) {
		if (!BIG_ENDIAN_PLATFORM) {
			copyMemory(buffer, offset, c.vector,
					DOUBLE_ARRAY_OFFSET + rowId * 8, total * 8);
		} else {
			ByteBuffer bb = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
			for (int i = 0; i < total; ++i) {
				c.vector[i + rowId] = bb.getDouble(offset - BYTE_ARRAY_OFFSET + (8 * i));
			}
		}
		offset += 8 * total;
	}

	@Override
	public byte readByte() {
		return (byte) readInteger();
	}

	@Override
	public final int readInteger() {
		int v = UNSAFE.getInt(buffer, (long) offset);
		if (BIG_ENDIAN_PLATFORM) {
			v = java.lang.Integer.reverseBytes(v);
		}
		offset += 4;
		return v;
	}

	@Override
	public final double readDouble() {
		double v;
		if (!BIG_ENDIAN_PLATFORM) {
			v = UNSAFE.getDouble(buffer, (long) offset);
		} else {
			v = byteBuffer.getDouble(offset - BYTE_ARRAY_OFFSET);
		}
		offset += 8;
		return v;
	}

	@Override
	public void readBooleans(int total, BooleanColumnVector c, int rowId) {
		for (int i = 0; i < total; i++) {
			c.vector[rowId + i] = readBoolean();
		}
	}

	@Override
	public final boolean readBoolean() {
		byte b = UNSAFE.getByte(buffer, (long) offset);
		boolean v = (b & (1 << bitOffset)) != 0;
		bitOffset += 1;
		if (bitOffset == 8) {
			bitOffset = 0;
			offset++;
		}
		return v;
	}

	@Override
	public final void readBinaries(int total, BytesColumnVector v, int rowId) {
		for (int i = 0; i < total; i++) {
			int len = readInteger();
			v.setVal(rowId + i, buffer, offset - BYTE_ARRAY_OFFSET, len);
			offset += len;
		}
	}

	@Override
	public final Binary readBinary(int len) {
		Binary result = Binary.fromConstantByteArray(buffer, offset - BYTE_ARRAY_OFFSET, len);
		offset += len;
		return result;
	}

	public final void readBytes(int total, ByteColumnVector c, int rowId) {
		for (int i = 0; i < total; i++) {
			// Bytes are stored as a 4-byte little endian int. Just read the first byte.
			c.vector[rowId + i] = UNSAFE.getByte(buffer, (long) offset);
			offset += 4;
		}
	}

	private void copyMemory(Object src, long srcOffset, Object dst, long dstOffset, long length) {
		while (length > 0) {
			long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
			UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
			length -= size;
			srcOffset += size;
			dstOffset += size;
		}
	}
}
