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
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.coders.Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.readIntoNullMask;
import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.writeNullMask;

/**
 * A {@link Coder} for {@link Row}.
 */
@Internal
public class RowCoder extends Coder<Row> {

	private static final long serialVersionUID = 1L;

	private final Coder<Object>[] fieldCoders;

	private transient ReusableDataInputView reuseInputStream;
	private transient ReusableDataOutputView reuseOutputStream;

	private transient boolean[] nullMask;

	@SuppressWarnings("unchecked")
	public RowCoder(Coder<?>[] fieldCoders) {
		this.fieldCoders = (Coder<Object>[]) Preconditions.checkNotNull(fieldCoders);
		this.reuseInputStream = new ReusableDataInputView();
		this.reuseOutputStream = new ReusableDataOutputView();
		this.nullMask = new boolean[fieldCoders.length];
	}

	public Coder<?>[] getFieldCoders() {
		return this.fieldCoders;
	}

	@Override
	public void encode(Row row, OutputStream outStream) throws IOException {
		int len = fieldCoders.length;

		if (row.getArity() != len) {
			throw new RuntimeException("Row arity of input element does not match coders.");
		}

		// write a null mask
		reuseOutputStream.reset(outStream);
		writeNullMask(len, row, reuseOutputStream);

		for (int i = 0; i < row.getArity(); i++) {
			Object o = row.getField(i);
			if (o != null) {
				fieldCoders[i].encode(o, outStream);
			}
		}
	}

	@Override
	public Row decode(InputStream inStream) throws IOException {
		int len = fieldCoders.length;

		// read null mask
		reuseInputStream.resetInputStream(inStream);
		readIntoNullMask(len, reuseInputStream, nullMask);

		Row row = new Row(len);
		for (int i = 0; i < len; i++) {
			if (nullMask[i]) {
				row.setField(i, null);
			}
			else {
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
}
