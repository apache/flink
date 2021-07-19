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

package org.apache.flink.ml.common.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.linalg.Vector;
import org.apache.flink.ml.common.utils.TableUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for the statistics.
 */
public class StatisticsUtil {

	/**
	 * Transform the vector to {@link DenseVector}, whether the vector is dense or sparse.
	 *
	 * @param vector the input vector
	 * @return the transformed {@link DenseVector}
	 */
	public static DenseVector toDenseVector(Vector vector) {
		if (vector instanceof DenseVector) {
			return (DenseVector) vector;
		} else {
			return ((SparseVector) vector).toDenseVector();
		}
	}

	/**
	 * get indices which type is number.
	 */
	static int[] getNumericalColIndices(TypeInformation[] colTypes) {
		List<Integer> numberColIndicesList = new ArrayList<>();
		for (int i = 0; i < colTypes.length; i++) {
			if (TableUtil.isNumber(colTypes[i])) {
				numberColIndicesList.add(i);
			}
		}

		int[] numberColIndices = new int[numberColIndicesList.size()];
		for (int i = 0; i < numberColIndices.length; i++) {
			numberColIndices[i] = numberColIndicesList.get(i);
		}
		return numberColIndices;
	}
}

