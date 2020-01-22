/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.vector.writable.WritableBytesVector;
import org.apache.flink.table.dataformat.vector.writable.WritableColumnVector;
import org.apache.flink.table.dataformat.vector.writable.WritableIntVector;
import org.apache.flink.table.dataformat.vector.writable.WritableLongVector;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Fixed length bytes {@link ColumnReader}, just for decimal.
 */
public class FixedLenBytesColumnReader<VECTOR extends WritableColumnVector> extends AbstractColumnReader<VECTOR> {

	private final int precision;

	public FixedLenBytesColumnReader(
			ColumnDescriptor descriptor,
			PageReader pageReader,
			int precision) throws IOException {
		super(descriptor, pageReader);
		checkTypeName(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
		this.precision = precision;
	}

	@Override
	protected void readBatch(int rowId, int num, VECTOR column) {
		int bytesLen = descriptor.getPrimitiveType().getTypeLength();
		if (Decimal.is32BitDecimal(precision)) {
			WritableIntVector intVector = (WritableIntVector) column;
			for (int i = 0; i < num; i++) {
				if (runLenDecoder.readInteger() == maxDefLevel) {
					intVector.setInt(rowId + i, (int) heapBinaryToLong(readDataBinary(bytesLen)));
				} else {
					intVector.setNullAt(rowId + i);
				}
			}
		} else if (Decimal.is64BitDecimal(precision)) {
			WritableLongVector longVector = (WritableLongVector) column;
			for (int i = 0; i < num; i++) {
				if (runLenDecoder.readInteger() == maxDefLevel) {
					longVector.setLong(rowId + i, heapBinaryToLong(readDataBinary(bytesLen)));
				} else {
					longVector.setNullAt(rowId + i);
				}
			}
		} else {
			WritableBytesVector bytesVector = (WritableBytesVector) column;
			for (int i = 0; i < num; i++) {
				if (runLenDecoder.readInteger() == maxDefLevel) {
					byte[] bytes = readDataBinary(bytesLen).getBytes();
					bytesVector.appendBytes(rowId + i, bytes, 0, bytes.length);
				} else {
					bytesVector.setNullAt(rowId + i);
				}
			}
		}
	}

	@Override
	protected void readBatchFromDictionaryIds(
			int rowId,
			int num,
			VECTOR column,
			WritableIntVector dictionaryIds) {
		if (Decimal.is32BitDecimal(precision)) {
			WritableIntVector intVector = (WritableIntVector) column;
			for (int i = rowId; i < rowId + num; ++i) {
				if (!intVector.isNullAt(i)) {
					Binary v = dictionary.decodeToBinary(dictionaryIds.getInt(i));
					intVector.setInt(i, (int) heapBinaryToLong(v));
				}
			}
		} else if (Decimal.is64BitDecimal(precision)) {
			WritableLongVector longVector = (WritableLongVector) column;
			for (int i = rowId; i < rowId + num; ++i) {
				if (!longVector.isNullAt(i)) {
					Binary v = dictionary.decodeToBinary(dictionaryIds.getInt(i));
					longVector.setLong(i, heapBinaryToLong(v));
				}
			}
		} else {
			WritableBytesVector bytesVector = (WritableBytesVector) column;
			for (int i = rowId; i < rowId + num; ++i) {
				if (!bytesVector.isNullAt(i)) {
					byte[] v = dictionary.decodeToBinary(dictionaryIds.getInt(i)).getBytes();
					bytesVector.appendBytes(i, v, 0, v.length);
				}
			}
		}
	}

	private long heapBinaryToLong(Binary binary) {
		ByteBuffer buffer = binary.toByteBuffer();
		byte[] bytes = buffer.array();
		int start = buffer.arrayOffset() + buffer.position();
		int end = buffer.arrayOffset() + buffer.limit();

		long unscaled = 0L;

		for (int i = start; i < end; i++) {
			unscaled = (unscaled << 8) | (bytes[i] & 0xff);
		}

		int bits = 8 * (end - start);
		return (unscaled << (64 - bits)) >> (64 - bits);
	}

	private Binary readDataBinary(int len) {
		ByteBuffer buffer = readDataBuffer(len);
		if (buffer.hasArray()) {
			return Binary.fromConstantByteArray(
					buffer.array(), buffer.arrayOffset() + buffer.position(), len);
		} else {
			byte[] bytes = new byte[len];
			buffer.get(bytes);
			return Binary.fromConstantByteArray(bytes);
		}
	}
}
