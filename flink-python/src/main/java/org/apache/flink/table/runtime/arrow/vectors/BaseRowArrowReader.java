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

package org.apache.flink.table.runtime.arrow.vectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.dataformat.vector.ColumnVector;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.util.Preconditions;

/**
 * {@link ArrowReader} which read the underlying Arrow format data as {@link BaseRow}.
 */
@Internal
public final class BaseRowArrowReader implements ArrowReader<BaseRow> {

	/**
	 * An array of vectors which are responsible for the deserialization of each column of the rows.
	 */
	private final ColumnVector[] columnVectors;

	/**
	 * Reusable row used to hold the deserialized result.
	 */
	private ColumnarRow reuseRow;

	public BaseRowArrowReader(ColumnVector[] columnVectors) {
		this.columnVectors = Preconditions.checkNotNull(columnVectors);
		this.reuseRow = new ColumnarRow();
	}

	/**
	 * Gets the column vectors.
	 */
	public ColumnVector[] getColumnVectors() {
		return columnVectors;
	}

	@Override
	public BaseRow read(int rowId) {
		reuseRow.setVectorizedColumnBatch(new VectorizedColumnBatch(columnVectors));
		reuseRow.setRowId(rowId);
		return reuseRow;
	}
}
