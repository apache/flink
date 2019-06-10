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

package org.apache.flink.ml.common.matrix;

/**
 * DenseMatrixUtil provides some operations over DenseMatrix.
 */
public class DenseMatrixUtil {

	/**
	 * Provides access to DenseMatrix's internal data buffer. Be aware that
	 * the data is stored in column major.
	 *
	 * @param mat The matrix.
	 * @return The matrix's internal data buffer.
	 */
	public static double[] getDataBuffer(DenseMatrix mat) {
		return mat.data;
	}

	/**
	 * Create a DenseMatrix using the provided data buffer.
	 *
	 * @param m      Number of rows.
	 * @param n      Number of columns.
	 * @param buffer The data buffer.
	 * @return A dense matrix.
	 */
	public static DenseMatrix createFromBuffer(int m, int n, double[] buffer) {
		DenseMatrix mat = new DenseMatrix();
		mat.data = buffer;
		mat.m = m;
		mat.n = n;
		return mat;
	}

	/**
	 * matC := matA .* matB .
	 */
	public static DenseMatrix elementWiseProduct(DenseMatrix matA, DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(matA.m, matA.n);
		DenseMatrix.apply(matA, matB, matC, ((a, b) -> a * b));
		return matC;
	}

	/**
	 * matC := matA ./ matB.
	 */
	public static DenseMatrix elementWiseDivide(DenseMatrix matA, DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(matA.m, matA.n);
		DenseMatrix.apply(matA, matB, matC, ((a, b) -> a / b));
		return matC;
	}
}

