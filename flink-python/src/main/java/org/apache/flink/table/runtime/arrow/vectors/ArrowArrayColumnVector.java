/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow.vectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ColumnarArrayData;
import org.apache.flink.table.data.vector.ArrayColumnVector;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.complex.ListVector;

/**
 * Arrow column vector for Array.
 */
@Internal
public final class ArrowArrayColumnVector implements ArrayColumnVector {

	/**
	 * Container which is used to store the sequence of array values of a column to read.
	 */
	private final ListVector listVector;
	private final ColumnVector elementVector;

	public ArrowArrayColumnVector(ListVector listVector, ColumnVector elementVector) {
		this.listVector = Preconditions.checkNotNull(listVector);
		this.elementVector = Preconditions.checkNotNull(elementVector);
	}

	@Override
	public ArrayData getArray(int i) {
		int index = i * ListVector.OFFSET_WIDTH;
		int start = listVector.getOffsetBuffer().getInt(index);
		int end = listVector.getOffsetBuffer().getInt(index + ListVector.OFFSET_WIDTH);
		return new ColumnarArrayData(elementVector, start, end - start);
	}

	@Override
	public boolean isNullAt(int i) {
		return listVector.isNull(i);
	}
}
