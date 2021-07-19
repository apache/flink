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
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.linalg.Vector;
import org.apache.flink.ml.common.mapper.MISOMapper;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.dataproc.vector.VectorAssemblerParams;
import org.apache.flink.table.api.TableSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * This mapper maps many columns to one vector. the columns should be vector or numerical columns.
 */
public class VectorAssemblerMapper extends MISOMapper {
	/**
	 * this variable using to judge the output vector format.
	 * if nnz * ratio > vector size, the vector format is denseVector, else sparseVector.
	 *
	 * <p>if number of values in the vector is less than 1/RATIO of its length, then use SparseVector.
	 * else use DenseVector.
	 */
	private static final double RATIO = 1.5;

	private enum HandleType {
		ERROR,
		KEEP,
		SKIP
	}

	/**
	 * the way to handle invalid input.
	 */
	private HandleType handleInvalid;

	public VectorAssemblerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.handleInvalid = HandleType.valueOf(params.get(VectorAssemblerParams.HANDLE_INVALID).toUpperCase());
	}

	@Override
	protected TypeInformation initOutputColType() {
		return VectorTypes.VECTOR;
	}

	/**
	 * each column of the input may be a number or a Vector.
	 */
	@Override
	protected Object map(Object[] input) {
		int pos = 0;
		Map<Integer, Double> map = new HashMap<>();
		// getVector the data, and write it in List.
		for (Object col : input) {
			if (null != col) {
				if (col instanceof Number) {
					map.put(pos++, ((Number) col).doubleValue());
				} else if (col instanceof Vector) {
					pos = appendVector((Vector) col, map, pos);
				} else {
					throw new UnsupportedOperationException("not support type of object.");
				}
			} else {
				switch (handleInvalid) {
					case ERROR:
						throw new NullPointerException("null value is found in vector assembler inputs.");
					case KEEP:
						map.put(pos++, Double.NaN);
						break;
					case SKIP:
					default:
				}
			}
		}

		/* form the vector, and finally toString it. */
		Vector vec = new SparseVector(pos, map);

		if (map.size() * RATIO > pos) {
			vec = ((SparseVector) vec).toDenseVector();
		}

		return vec;
	}

	private int appendVector(Vector vec, Map<Integer, Double> map, int pos) {
		if (vec instanceof SparseVector) {
			SparseVector sv = (SparseVector) vec;
			int[] idx = sv.getIndices();
			double[] values = sv.getValues();
			for (int j = 0; j < idx.length; ++j) {
				map.put(pos + idx[j], values[j]);
			}
			pos += sv.size();
		} else if (vec instanceof DenseVector) {
			DenseVector dv = (DenseVector) vec;
			for (int j = 0; j < dv.size(); ++j) {
				map.put(pos++, dv.get(j));
			}
		}
		return pos;
	}
}
