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
import org.apache.flink.ml.common.linalg.VectorUtil;
import org.apache.flink.ml.common.mapper.SISOMapper;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.dataproc.vector.VectorElementwiseProductParams;
import org.apache.flink.table.api.TableSchema;

/**
 * This mapper maps a vector to a new vector with special scale.
 */
public class VectorElementwiseProductMapper extends SISOMapper {
	private Vector scalingVector;

	public VectorElementwiseProductMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.scalingVector = VectorUtil.parse(this.params.get(VectorElementwiseProductParams.SCALING_VECTOR));
	}

	@Override
	protected TypeInformation initOutputColType() {
		return VectorTypes.VECTOR;
	}

	@Override
	protected Object map(Object input) {
		if (null == input) {
			return null;
		}

		Vector vector = (Vector) input;

		if (vector instanceof DenseVector) {
			double[] vec = ((DenseVector) vector).getData();
			for (int i = 0; i < vec.length; ++i) {
				vec[i] = vec[i] * scalingVector.get(i);
			}
		} else {
			SparseVector vec = (SparseVector) vector;
			double[] vecValues = vec.getValues();
			int[] vecIndices = vec.getIndices();

			for (int i = 0; i < vecValues.length; ++i) {
				vecValues[i] *= scalingVector.get(vecIndices[i]);
			}
		}
		return vector;
	}

}
