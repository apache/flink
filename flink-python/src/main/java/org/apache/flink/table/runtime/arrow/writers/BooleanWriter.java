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

import org.apache.arrow.vector.BitVector;

/**
 * {@link ArrowFieldWriter} for Boolean.
 */
@Internal
public abstract class BooleanWriter<T> extends ArrowFieldWriter<T> {

	public static BooleanWriter<RowData> forRow(BitVector bitVector) {
		return new BooleanWriterForRow(bitVector);
	}

	public static BooleanWriter<ArrayData> forArray(BitVector bitVector) {
		return new BooleanWriterForArray(bitVector);
	}

	// ------------------------------------------------------------------------------------------

	private BooleanWriter(BitVector bitVector) {
		super(bitVector);
	}

	abstract boolean isNullAt(T in, int ordinal);

	abstract boolean readBoolean(T in, int ordinal);

	@Override
	public void doWrite(T in, int ordinal) {
		if (isNullAt(in, ordinal)) {
			((BitVector) getValueVector()).setNull(getCount());
		} else if (readBoolean(in, ordinal)) {
			((BitVector) getValueVector()).setSafe(getCount(), 1);
		} else {
			((BitVector) getValueVector()).setSafe(getCount(), 0);
		}
	}

	// ------------------------------------------------------------------------------------------

	/**
	 * {@link BooleanWriter} for {@link RowData} input.
	 */
	public static final class BooleanWriterForRow extends BooleanWriter<RowData> {

		private BooleanWriterForRow(BitVector bitVector) {
			super(bitVector);
		}

		@Override
		boolean isNullAt(RowData in, int ordinal) {
			return in.isNullAt(ordinal);
		}

		@Override
		boolean readBoolean(RowData in, int ordinal) {
			return in.getBoolean(ordinal);
		}
	}

	/**
	 * {@link BooleanWriter} for {@link ArrayData} input.
	 */
	public static final class BooleanWriterForArray extends BooleanWriter<ArrayData> {

		private BooleanWriterForArray(BitVector bitVector) {
			super(bitVector);
		}

		@Override
		boolean isNullAt(ArrayData in, int ordinal) {
			return in.isNullAt(ordinal);
		}

		@Override
		boolean readBoolean(ArrayData in, int ordinal) {
			return in.getBoolean(ordinal);
		}
	}
}
