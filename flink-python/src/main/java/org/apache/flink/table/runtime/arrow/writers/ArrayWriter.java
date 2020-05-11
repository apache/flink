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

package org.apache.flink.table.runtime.arrow.writers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.complex.ListVector;

/**
 * {@link ArrowFieldWriter} for Array.
 */
@Internal
public abstract class ArrayWriter<T> extends ArrowFieldWriter<T> {

	public static ArrayWriter<RowData> forRow(ListVector listVector, ArrowFieldWriter<ArrayData> elementWriter) {
		return new ArrayWriterForRow(listVector, elementWriter);
	}

	public static ArrayWriter<ArrayData> forArray(ListVector listVector, ArrowFieldWriter<ArrayData> elementWriter) {
		return new ArrayWriterForArray(listVector, elementWriter);
	}

	// ------------------------------------------------------------------------------------------

	private final ArrowFieldWriter<ArrayData> elementWriter;

	private ArrayWriter(ListVector listVector, ArrowFieldWriter<ArrayData> elementWriter) {
		super(listVector);
		this.elementWriter = Preconditions.checkNotNull(elementWriter);
	}

	abstract boolean isNullAt(T in, int ordinal);

	abstract ArrayData readArray(T in, int ordinal);

	@Override
	public void doWrite(T in, int ordinal) {
		if (!isNullAt(in, ordinal)) {
			((ListVector) getValueVector()).startNewValue(getCount());
			ArrayData array = readArray(in, ordinal);
			for (int i = 0; i < array.size(); i++) {
				elementWriter.write(array, i);
			}
			((ListVector) getValueVector()).endValue(getCount(), array.size());
		}
	}

	@Override
	public void finish() {
		super.finish();
		elementWriter.finish();
	}

	@Override
	public void reset() {
		super.reset();
		elementWriter.reset();
	}

	// ------------------------------------------------------------------------------------------

	/**
	 * {@link ArrayWriter} for {@link RowData} input.
	 */
	public static final class ArrayWriterForRow extends ArrayWriter<RowData> {

		private ArrayWriterForRow(ListVector listVector, ArrowFieldWriter<ArrayData> elementWriter) {
			super(listVector, elementWriter);
		}

		@Override
		boolean isNullAt(RowData in, int ordinal) {
			return in.isNullAt(ordinal);
		}

		@Override
		ArrayData readArray(RowData in, int ordinal) {
			return in.getArray(ordinal);
		}
	}

	/**
	 * {@link ArrayWriter} for {@link ArrayData} input.
	 */
	public static final class ArrayWriterForArray extends ArrayWriter<ArrayData> {

		private ArrayWriterForArray(ListVector listVector, ArrowFieldWriter<ArrayData> elementWriter) {
			super(listVector, elementWriter);
		}

		@Override
		boolean isNullAt(ArrayData in, int ordinal) {
			return in.isNullAt(ordinal);
		}

		@Override
		ArrayData readArray(ArrayData in, int ordinal) {
			return in.getArray(ordinal);
		}
	}
}
