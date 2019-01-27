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

package org.apache.flink.api.common.functions;

import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * A {@link Merger} for rows which merges two rows by merging the objects in
 * corresponding fields with given field mergers.
 */
public class RowMerger implements Merger<Row> {

	private static final long serialVersionUID = 1L;

	/**
	 * The mergers for the objects in the fields.
	 */
	private final Merger<?>[] fieldMergers;

	/**
	 * Constructor with the mergers for the objects in the fields.
	 *
	 * @param fieldMergers The mergers for the objects in the fields.
	 */
	public RowMerger(Merger<?>[] fieldMergers) {
		Preconditions.checkNotNull(fieldMergers);
		for (Merger<?> fieldMerger : fieldMergers) {
			Preconditions.checkNotNull(fieldMerger);
		}

		this.fieldMergers = fieldMergers;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Row merge(Row row1, Row row2) {
		Preconditions.checkNotNull(row1);
		Preconditions.checkArgument(row1.getArity() == fieldMergers.length);
		Preconditions.checkNotNull(row2);
		Preconditions.checkArgument(row2.getArity() == fieldMergers.length);

		Row mergedRow = new Row(fieldMergers.length);
		for (int i = 0; i < fieldMergers.length; ++i) {
			Object fieldValue1 = row1.getField(i);
			Object fieldValue2 = row2.getField(i);

			Merger<Object> fieldMerger = (Merger<Object>) fieldMergers[i];
			Object mergedFieldValue = fieldMerger.merge(fieldValue1, fieldValue2);
			mergedRow.setField(i, mergedFieldValue);
		}

		return mergedRow;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		RowMerger rowMerger = (RowMerger) o;
		return Arrays.equals(fieldMergers, rowMerger.fieldMergers);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(fieldMergers);
	}

	@Override
	public String toString() {
		return "RowMerger{" +
			"fieldMergers=" + Arrays.toString(fieldMergers) +
		"}";
	}
}

