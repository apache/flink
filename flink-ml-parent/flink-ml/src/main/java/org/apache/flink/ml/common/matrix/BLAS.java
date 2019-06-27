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
 * BLAS.
 */
public class BLAS {
	private static com.github.fommil.netlib.BLAS blas = com.github.fommil.netlib.BLAS.getInstance();

	/**
	 * y += a * x  .
	 */
	public static void daxpy(double a, double[] x, double[] y) {
		blas.daxpy(x.length, a, x, 1, y, 1);
	}

	public static void saxpy(float a, float[] x, float[] y) {
		blas.saxpy(x.length, a, x, 1, y, 1);
	}

	public static void axpy(double a, DenseVector x, DenseVector y) {
		daxpy(a, x.getData(), y.getData());
	}

	/**
	 * x \cdot y   .
	 */
	public static double ddot(double[] x, double[] y) {
		return blas.ddot(x.length, x, 1, y, 1);
	}

	public static float sdot(float[] x, float[] y) {
		return blas.sdot(x.length, x, 1, y, 1);
	}

	public static double dot(DenseVector x, DenseVector y) {
		return ddot(x.getData(), y.getData());
	}

	/**
	 * x = x * a  .
	 */
	public static void dscal(double a, double[] x) {
		blas.dscal(x.length, a, x, 1);
	}

	public static void sscal(float a, float[] x) {
		blas.sscal(x.length, a, x, 1);
	}

	public static void scal(double a, DenseVector x) {
		dscal(a, x.getData());
	}

	/**
	 * || x - y ||^2  .
	 */
	public static double ddsquared(double[] x, double[] y) {
		double s = 0.;
		for (int i = 0; i < x.length; i++) {
			double d = x[i] - y[i];
			s += d * d;
		}
		return s;
	}

	/**
	 * | x - y |  .
	 */
	public static double ddabs(double[] x, double[] y) {
		double s = 0.;
		for (int i = 0; i < x.length; i++) {
			double d = x[i] - y[i];
			s += Math.abs(d);
		}
		return s;
	}

	public static float sdsquared(float[] x, float[] y) {
		float s = 0.F;
		for (int i = 0; i < x.length; i++) {
			float d = x[i] - y[i];
			s += d * d;
		}
		return s;
	}

	public static double dsquared(DenseVector x, DenseVector y) {
		return ddsquared(x.getData(), y.getData());
	}
}
