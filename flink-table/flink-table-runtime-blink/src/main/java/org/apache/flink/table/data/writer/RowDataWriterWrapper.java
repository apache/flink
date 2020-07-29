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

package org.apache.flink.table.data.writer;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * A utility class for reusing row writers in row data serializer implementations.
 */
public class RowDataWriterWrapper {

	private final LogicalType[] types;

	private final BinaryRowData reuseRow;
	private final BinaryRowWriter reuseWriter;
	private final BinaryWriter.NullSetter[] reuseNullSetters;
	private final BinaryWriter.ValueSetter[] reuseValueSetters;

	public RowDataWriterWrapper(LogicalType[] types) {
		this.types = types;

		reuseRow = new BinaryRowData(types.length);
		reuseWriter = new BinaryRowWriter(reuseRow);
		reuseNullSetters = new BinaryWriter.NullSetter[types.length];
		for (int i = 0; i < reuseNullSetters.length; i++) {
			reuseNullSetters[i] = BinaryWriter.createNullSetter(types[i]);
		}
		reuseValueSetters = new BinaryWriter.ValueSetter[types.length];
		for (int i = 0; i < reuseValueSetters.length; i++) {
			reuseValueSetters[i] = BinaryWriter.createValueSetter(types[i]);
		}
	}

	public BinaryRowData write(RowData row) {
		reuseWriter.reset();

		reuseWriter.writeRowKind(row.getRowKind());
		for (int i = 0; i < types.length; i++) {
			if (row.isNullAt(i)) {
				reuseNullSetters[i].setNull(reuseWriter, i);
			} else {
				reuseValueSetters[i].setValue(reuseWriter, i, RowData.get(row, i, types[i]));
			}
		}
		reuseWriter.complete();

		return reuseRow;
	}
}
