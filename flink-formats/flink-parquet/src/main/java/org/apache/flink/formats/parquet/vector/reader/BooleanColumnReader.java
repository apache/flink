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

import org.apache.flink.table.dataformat.vector.writable.WritableBooleanVector;
import org.apache.flink.table.dataformat.vector.writable.WritableIntVector;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

/**
 * Boolean {@link ColumnReader}.
 */
public class BooleanColumnReader extends AbstractColumnReader<WritableBooleanVector> {

	/**
	 * Parquet use a bit to store booleans, so we need split a byte to 8 boolean.
	 */
	private int bitOffset;
	private byte currentByte = 0;

	public BooleanColumnReader(
			ColumnDescriptor descriptor,
			PageReader pageReader) throws IOException {
		super(descriptor, pageReader);
		checkTypeName(PrimitiveType.PrimitiveTypeName.BOOLEAN);
	}

	@Override
	protected boolean supportLazyDecode() {
		return true;
	}

	@Override
	protected void afterReadPage() {
		bitOffset = 0;
		currentByte = 0;
	}

	@Override
	protected void readBatchFromDictionaryIds(int rowId, int num, WritableBooleanVector column,
			WritableIntVector dictionaryIds) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void readBatch(int rowId, int num, WritableBooleanVector column) {
		int left = num;
		while (left > 0) {
			if (runLenDecoder.currentCount == 0) {
				runLenDecoder.readNextGroup();
			}
			int n = Math.min(left, runLenDecoder.currentCount);
			switch (runLenDecoder.mode) {
				case RLE:
					if (runLenDecoder.currentValue == maxDefLevel) {
						for (int i = 0; i < n; i++) {
							column.setBoolean(rowId + i, readBoolean());
						}
					} else {
						column.setNulls(rowId, n);
					}
					break;
				case PACKED:
					for (int i = 0; i < n; ++i) {
						if (runLenDecoder.currentBuffer[runLenDecoder.currentBufferIdx++] == maxDefLevel) {
							column.setBoolean(rowId + i, readBoolean());
						} else {
							column.setNullAt(rowId + i);
						}
					}
					break;
			}
			rowId += n;
			left -= n;
			runLenDecoder.currentCount -= n;
		}
	}

	private boolean readBoolean() {
		if (bitOffset == 0) {
			try {
				currentByte = (byte) dataInputStream.read();
			} catch (IOException e) {
				throw new ParquetDecodingException("Failed to read a byte", e);
			}
		}

		boolean v = (currentByte & (1 << bitOffset)) != 0;
		bitOffset += 1;
		if (bitOffset == 8) {
			bitOffset = 0;
		}
		return v;
	}
}
