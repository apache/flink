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

import org.apache.arrow.vector.SmallIntVector;

/**
 * {@link ArrowFieldWriter} for SmallInt.
 */
@Internal
public abstract class SmallIntWriter<T> extends ArrowFieldWriter<T> {

	public static SmallIntWriter<RowData> forRow(SmallIntVector intVector) {
		return new SmallIntWriterForRow(intVector);
	}

	public static SmallIntWriter<ArrayData> forArray(SmallIntVector intVector) {
		return new SmallIntWriterForArray(intVector);
	}

	// ------------------------------------------------------------------------------------------

	private SmallIntWriter(SmallIntVector intVector) {
		super(intVector);
	}

	abstract boolean isNullAt(T in, int ordinal);

	abstract short readShort(T in, int ordinal);

	@Override
	public void doWrite(T in, int ordinal) {
		if (isNullAt(in, ordinal)) {
			((SmallIntVector) getValueVector()).setNull(getCount());
		} else {
			((SmallIntVector) getValueVector()).setSafe(getCount(), readShort(in, ordinal));
		}
	}

	// ------------------------------------------------------------------------------------------

	/**
	 * {@link SmallIntWriter} for {@link RowData} input.
	 */
	public static final class SmallIntWriterForRow extends SmallIntWriter<RowData> {

		private SmallIntWriterForRow(SmallIntVector intVector) {
			super(intVector);
		}

		@Override
		boolean isNullAt(RowData in, int ordinal) {
			return in.isNullAt(ordinal);
		}

		@Override
		short readShort(RowData in, int ordinal) {
			return in.getShort(ordinal);
		}
	}

	/**
	 * {@link SmallIntWriter} for {@link ArrayData} input.
	 */
	public static final class SmallIntWriterForArray extends SmallIntWriter<ArrayData> {

		private SmallIntWriterForArray(SmallIntVector intVector) {
			super(intVector);
		}

		@Override
		boolean isNullAt(ArrayData in, int ordinal) {
			return in.isNullAt(ordinal);
		}

		@Override
		short readShort(ArrayData in, int ordinal) {
			return in.getShort(ordinal);
		}
	}
}
