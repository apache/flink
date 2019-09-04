/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.linalg.Vector;
import org.apache.flink.ml.common.mapper.Mapper;
import org.apache.flink.ml.common.utils.OutputColsHelper;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.params.dataproc.vector.VectorToColumnsParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * This mapper maps vector to table columns.
 */
public class VectorToColumnsMapper extends Mapper {
	private int colSize;
	private int idx;
	private OutputColsHelper outputColsHelper;

	public VectorToColumnsMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		String selectedColName = this.params.get(VectorToColumnsParams.SELECTED_COL);
		idx = TableUtil.findColIndex(dataSchema.getFieldNames(), selectedColName);
		if (idx < 0) {
			throw new IllegalArgumentException("Can not find column: " + selectedColName);
		}
		String[] outputColNames = this.params.get(VectorToColumnsParams.OUTPUT_COLS);
		if (outputColNames == null) {
			throw new IllegalArgumentException("VectorToTable: outputColNames must set.");
		}
		this.colSize = outputColNames.length;
		TypeInformation[] types = new TypeInformation[colSize];
		for (int i = 0; i < colSize; ++i) {
			types[i] = Types.DOUBLE;
		}
		this.outputColsHelper = new OutputColsHelper(dataSchema, outputColNames, types,
			this.params.get(VectorToColumnsParams.RESERVED_COLS));
	}

	@Override
	public Row map(Row row) {
		Row result = new Row(colSize);
		Object obj = row.getField(idx);
		if (null == obj) {
			for (int i = 0; i < colSize; i++) {
				result.setField(i, null);
			}
			return outputColsHelper.getResultRow(row, result);
		}

		Vector vec = (Vector) obj;

		if (vec instanceof SparseVector) {
			for (int i = 0; i < colSize; ++i) {
				result.setField(i, 0.0);
			}
			SparseVector sparseVector = (SparseVector) vec;
			int nnz = sparseVector.numberOfValues();
			int[] indices = sparseVector.getIndices();
			double[] values = sparseVector.getValues();
			for (int i = 0; i < nnz; ++i) {
				if (indices[i] < colSize) {
					result.setField(indices[i], values[i]);
				} else {
					break;
				}
			}
		} else {
			DenseVector denseVector = (DenseVector) vec;
			for (int i = 0; i < colSize; ++i) {
				result.setField(i, denseVector.get(i));
			}
		}
		return outputColsHelper.getResultRow(row, result);
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}
}
