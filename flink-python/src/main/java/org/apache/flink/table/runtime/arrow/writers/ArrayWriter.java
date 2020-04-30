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
import org.apache.flink.table.dataformat.BaseArray;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.complex.ListVector;

/**
 * {@link ArrowFieldWriter} for Array.
 */
@Internal
public final class ArrayWriter<T1 extends TypeGetterSetters, T2 extends TypeGetterSetters> extends ArrowFieldWriter<T1> {

	private final ArrowFieldWriter<T2> elementWriter;

	public ArrayWriter(ListVector listVector, ArrowFieldWriter<T2> elementWriter) {
		super(listVector);
		this.elementWriter = Preconditions.checkNotNull(elementWriter);
	}

	@Override
	public void doWrite(T1 row, int ordinal) {
		if (!row.isNullAt(ordinal)) {
			((ListVector) getValueVector()).startNewValue(getCount());
			BaseArray array = row.getArray(ordinal);
			for (int i = 0; i < array.numElements(); i++) {
				elementWriter.write((T2) array, i);
			}
			((ListVector) getValueVector()).endValue(getCount(), array.numElements());
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
}
