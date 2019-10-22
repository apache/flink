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

package org.apache.flink.table.runtime.typeutils.coders;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.coders.Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.readIntoNullMask;

/**
 * A {@link Coder} for {@link BaseRow}. It should be noted that the header will not be encoded.
 */
@Internal
public class BaseRowCoder extends Coder<BaseRow> {

	private static final long serialVersionUID = 1L;

	private final Coder<Object>[] fieldCoders;
	private final LogicalType[] fieldTypes;

	private transient ReusableDataInputView reusableInputStream;
	private transient ReusableDataOutputView reusableOutputStream;

	private transient boolean[] nullMask;

	@SuppressWarnings("unchecked")
	public BaseRowCoder(Coder<?>[] fieldCoders, LogicalType[] fieldTypes) {
		this.fieldCoders = (Coder<Object>[]) Preconditions.checkNotNull(fieldCoders);
		this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
		this.reusableInputStream = new ReusableDataInputView();
		this.reusableOutputStream = new ReusableDataOutputView();
		this.nullMask = new boolean[fieldCoders.length];
	}

	public Coder<?>[] getFieldCoders() {
		return this.fieldCoders;
	}

	@Override
	public void encode(BaseRow row, OutputStream outStream) throws IOException {
		int len = fieldCoders.length;

		if (row.getArity() != len) {
			throw new RuntimeException("Row arity of input element does not match coders.");
		}

		// write a null mask
		reusableOutputStream.reset(outStream);
		writeNullMask(len, row, reusableOutputStream);

		for (int i = 0; i < row.getArity(); i++) {
			if (!row.isNullAt(i)) {
				// TODO: support BaseRow natively in Python, then we can eliminate the redundant serialize/deserialize
				fieldCoders[i].encode(TypeGetterSetters.get(row, i, fieldTypes[i]), outStream);
			}
		}
	}

	@Override
	public BaseRow decode(InputStream inStream) throws IOException {
		int len = fieldCoders.length;

		// read null mask
		reusableInputStream.resetInputStream(inStream);
		readIntoNullMask(len, reusableInputStream, nullMask);

		GenericRow row = new GenericRow(fieldCoders.length);
		for (int i = 0; i < row.getArity(); i++) {
			if (nullMask[i]) {
				row.setField(i, null);
			} else {
				row.setField(i, fieldCoders[i].decode(inStream));
			}
		}
		return row;
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		return Collections.emptyList();
	}

	@Override
	public void verifyDeterministic() {}

	@Override
	public boolean consistentWithEquals() {
		return true;
	}

	private static void writeNullMask(int len, BaseRow value, DataOutputView target) throws IOException {
		int b = 0x00;
		int bytePos = 0;

		int fieldPos = 0;
		int numPos = 0;
		while (fieldPos < len) {
			b = 0x00;
			// set bits in byte
			bytePos = 0;
			numPos = Math.min(8, len - fieldPos);
			while (bytePos < numPos) {
				b = b << 1;
				// set bit if field is null
				if (value.isNullAt(fieldPos + bytePos)) {
					b |= 0x01;
				}
				bytePos += 1;
			}
			fieldPos += numPos;
			// shift bits if last byte is not completely filled
			b <<= (8 - bytePos);
			// write byte
			target.writeByte(b);
		}
	}
}
