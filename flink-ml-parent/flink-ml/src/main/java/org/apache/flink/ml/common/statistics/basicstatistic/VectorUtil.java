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

package org.apache.flink.ml.common.statistics.basicstatistic;

import org.apache.flink.ml.common.matrix.DenseMatrix;
import org.apache.flink.ml.common.matrix.DenseVector;

/**
 * Util of vector operations.
 *
 * <p>- DenseVector extension.
 *
 * <p>- DenseMatrix extension.
 */
public class VectorUtil {

	public static DenseVector normL1Equal(DenseVector dv) {
		double[] data = dv.getData();
		for (int i = 0; i < dv.size(); i++) {
			data[i] = Math.abs(data[i]);
		}
		return dv;
	}

	/**
	 * return left + normL1(right).
	 * it will change left, right will not be change
	 */
	public static DenseVector plusNormL1(DenseVector left, DenseVector right) {
		double[] leftData = left.getData();
		double[] rightData = right.getData();
		if (left.size() >= right.size()) {
			for (int i = 0; i < right.size(); i++) {
				leftData[i] += Math.abs(rightData[i]);
			}
			return left;
		} else {
			double[] data = rightData.clone();
			for (int i = 0; i < left.size(); i++) {
				data[i] = Math.abs(data[i]) + leftData[i];
			}
			return new DenseVector(data);
		}
	}

	/**
	 * return left + sum2(right).
	 * it will change left, right will not be change
	 */
	public static DenseVector plusSum2(DenseVector left, DenseVector right) {
		double[] leftData = left.getData();
		double[] rightData = right.getData();
		if (left.size() >= right.size()) {
			for (int i = 0; i < right.size(); i++) {
				leftData[i] += rightData[i] * rightData[i];
			}
			return left;
		} else {
			double[] data = rightData.clone();
			for (int i = 0; i < left.size(); i++) {
				data[i] = data[i] * data[i] + leftData[i];
			}
			return new DenseVector(data);
		}
	}

	/**
	 * left = left + right.
	 * reutrn left
	 */
	public static DenseVector plusEqual(DenseVector left, DenseVector right) {
		double[] leftData = left.getData();
		double[] rightData = right.getData();
		if (left.size() >= right.size()) {
			for (int i = 0; i < right.size(); i++) {
				leftData[i] += rightData[i];
			}
			return left;
		} else {
			double[] data = rightData.clone();
			for (int i = 0; i < left.size(); i++) {
				data[i] += leftData[i];
			}
			return new DenseVector(data);
		}
	}

	public static DenseVector minEqual(DenseVector left, DenseVector right) {
		double[] leftData = left.getData();
		double[] rightData = right.getData();
		if (left.size() >= right.size()) {
			for (int i = 0; i < right.size(); i++) {
				leftData[i] = Math.min(leftData[i], rightData[i]);
			}
			return left;
		} else {
			double[] data = rightData.clone();
			for (int i = 0; i < left.size(); i++) {
				data[i] = Math.min(leftData[i], rightData[i]);
			}
			return new DenseVector(data);
		}
	}

	public static DenseVector maxEqual(DenseVector left, DenseVector right) {
		double[] leftData = left.getData();
		double[] rightData = right.getData();
		if (left.size() >= right.size()) {
			for (int i = 0; i < right.size(); i++) {
				leftData[i] = Math.max(leftData[i], rightData[i]);
			}
			return left;
		} else {
			double[] data = rightData.clone();
			for (int i = 0; i < left.size(); i++) {
				data[i] = Math.max(leftData[i], rightData[i]);
			}
			return new DenseVector(data);
		}
	}

	public static DenseMatrix plusEqual(DenseMatrix left, DenseMatrix right) {
		for (int i = 0; i < right.numRows(); i++) {
			for (int j = 0; j < right.numCols(); j++) {
				left.add(i, j, right.get(i, j));
			}
		}
		return left;
	}

}
