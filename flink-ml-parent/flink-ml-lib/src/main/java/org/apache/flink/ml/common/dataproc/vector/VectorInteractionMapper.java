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
import org.apache.flink.table.api.TableSchema;

/**
 * This mapper maps two vectors to one with interact operation.
 */
public class VectorInteractionMapper extends MISOMapper {

	public VectorInteractionMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	protected TypeInformation initOutputColType() {
		return VectorTypes.VECTOR;
	}

	@Override
	protected Object map(Object[] input) {
		if (input.length != 2) {
			throw new IllegalArgumentException("VectorInteraction only support two input columns.");
		}

		if (input[0] == null || input[1] == null) {
			return null;
		}

		Vector vector1 = (Vector) input[0];
		Vector vector2 = (Vector) input[1];

		if (vector1 instanceof SparseVector) {
			if (vector2 instanceof DenseVector) {
				throw new IllegalArgumentException("Make sure the two input vectors are both dense or sparse.");
			}
			SparseVector sparseVector = (SparseVector) vector1;
			int vecSize = sparseVector.size();
			int[] indices = sparseVector.getIndices();
			double[] values = sparseVector.getValues();
			SparseVector scalingVector = (SparseVector) vector2;
			int scalingSize = scalingVector.size();
			int[] scalingIndices = scalingVector.getIndices();
			double[] scalingValues = scalingVector.getValues();
			double[] interactionValues = new double[scalingIndices.length * indices.length];
			int[] interactionIndices = new int[scalingIndices.length * indices.length];
			for (int i = 0; i < indices.length; ++i) {
				int idxBase = i * scalingIndices.length;
				for (int j = 0; j < scalingIndices.length; ++j) {
					int idx = idxBase + j;
					interactionIndices[idx] = vecSize * scalingIndices[j] + indices[i];
					interactionValues[idx] = values[i] * scalingValues[j];
				}
			}
			return new SparseVector(vecSize * scalingSize, interactionIndices, interactionValues);
		} else {
			if (vector2 instanceof SparseVector) {
				throw new IllegalArgumentException("Make sure the two input vectors are both dense or sparse.");
			}
			double[] vecArray = ((DenseVector) vector1).getData();
			double[] scalingArray = ((DenseVector) vector2).getData();
			DenseVector inter = new DenseVector(vecArray.length * scalingArray.length);
			double[] interArray = inter.getData();
			for (int i = 0; i < vecArray.length; ++i) {
				int idxBase = i * scalingArray.length;
				for (int j = 0; j < scalingArray.length; ++j) {
					interArray[idxBase + j] = vecArray[i] * scalingArray[j];
				}
			}
			return inter;
		}
	}
}
