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

package org.apache.flink.ml.common.ml.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.linalg.Vector;
import org.apache.flink.ml.common.linalg.VectorIterator;
import org.apache.flink.ml.common.mapper.SISOMapper;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.ml.feature.BinarizerParams;
import org.apache.flink.table.api.TableSchema;

import java.lang.reflect.Constructor;
import java.util.Arrays;

/**
 * Binarize a continuous variable using a threshold. The features greater than threshold, will be binarized 1.0 and the
 * features equal to or less than threshold, will be binarized to 0.
 *
 * <p>Support Vector input and Number input.
 */
public class BinarizerMapper extends SISOMapper {
	private double threshold;
	private TypeInformation selectedColType;
	private static final double RATIO = 1.5;
	private Object objectValue0, objectValue1;

	public BinarizerMapper(TableSchema dataSchema, Params params) throws Exception {
		super(dataSchema, params);
		this.threshold = this.params.get(BinarizerParams.THRESHOLD);

		selectedColType = TableUtil.findColType(
			dataSchema,
			this.params.get(BinarizerParams.SELECTED_COL)
		);

		if (TableUtil.isNumber(selectedColType)) {
			Constructor constructor = selectedColType.getTypeClass().getConstructor(String.class);
			objectValue0 = constructor.newInstance("0");
			objectValue1 = constructor.newInstance("1");
		}
	}

	@Override
	protected TypeInformation initOutputColType() {
		final TypeInformation<?> selectedColType = TableUtil.findColType(
			dataSchema,
			this.params.get(BinarizerParams.SELECTED_COL)
		);

		if (TableUtil.isNumber(selectedColType)) {
			return selectedColType;
		}

		return VectorTypes.VECTOR;
	}

	/**
	 * If input is a vector, in the case of dense vector, all the features are compared with threshold, and return a
	 * vector in either dense or sparse format, whichever uses less storage. If input is a sparseVector, only compare
	 * those non-zero features, and returns a sparse vector.
	 *
	 * @param input data input, support number or Vector.
	 * @return If input is a number, compare the number with threshold.
	 * @throws IllegalArgumentException input is neither number nor vector.
	 */
	@Override
	protected Object map(Object input) throws Exception {
		if (null == input) {
			return null;
		}
		if (TableUtil.isNumber(selectedColType)) {
			return ((Number) input).doubleValue() > threshold ? objectValue1 : objectValue0;
		} else if (TableUtil.isVector(selectedColType)) {
			Vector parseVector = (Vector) input;
			if (null == parseVector) {
				return null;
			}
			if (parseVector instanceof SparseVector) {
				SparseVector vec = (SparseVector) parseVector;
				VectorIterator vectorIterator = vec.iterator();
				int[] newIndices = new int[vec.numberOfValues()];
				int pos = 0;
				while (vectorIterator.hasNext()) {
					if (vectorIterator.getValue() > threshold) {
						newIndices[pos++] = vectorIterator.getIndex();
					}
					vectorIterator.next();
				}
				double[] newValues = new double[pos];
				Arrays.fill(newValues, 1.0);
				return new SparseVector(vec.size(), Arrays.copyOf(newIndices, pos), newValues);
			} else {
				DenseVector vec = (DenseVector) parseVector;
				double[] data = vec.getData();
				int[] newIndices = new int[vec.size()];
				int pos = 0;
				for (int i = 0; i < vec.size(); i++) {
					if (data[i] > threshold) {
						newIndices[pos++] = i;
						data[i] = 1.0;
					} else {
						data[i] = 0.0;
					}
				}
				if (pos * RATIO > vec.size()) {
					return vec;
				} else {
					double[] newValues = new double[pos];
					Arrays.fill(newValues, 1.0);
					return new SparseVector(vec.size(), Arrays.copyOf(newIndices, pos), newValues);
				}
			}
		} else {
			throw new IllegalArgumentException("Only support Number and vector!");
		}
	}
}
