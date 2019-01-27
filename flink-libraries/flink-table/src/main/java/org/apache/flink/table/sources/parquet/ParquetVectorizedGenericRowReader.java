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

package org.apache.flink.table.sources.parquet;

import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * This reader is used to read a {@link Row} from input split.
 */
public class ParquetVectorizedGenericRowReader extends ParquetVectorizedReader {

	/**
	 * the index of current row which will be returned.
	 */
	private int rowIdx = 0;
	/**
	 * the size of current accessible batch.
	 */
	private int batchSize = 0;

	public ParquetVectorizedGenericRowReader(InternalType[] fieldTypes, String[] fieldNames) {
		super(fieldTypes, fieldNames);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (batchSize == 0 || rowIdx >= batchSize) {
			if (!nextBatch()) {
				return false;
			} else {
				batchSize = columnarBatch.getNumRows();
				rowIdx = 0;
			}
		}
		++rowIdx;
		return true;
	}

	@Override
	public Object getCurrentValue() throws IOException, InterruptedException {
		GenericRow row = new GenericRow(columnarBatch.getArity());
		for (int colId = 0; colId < columnarBatch.getArity(); colId++) {
			row.update(colId, columnarBatch.getObject(rowIdx - 1, colId));
		}
		return row;
	}
}
