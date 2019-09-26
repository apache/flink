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

import org.apache.flink.ml.common.linalg.BLAS;
import org.apache.flink.ml.common.linalg.DenseMatrix;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.linalg.Vector;

import com.github.fommil.netlib.LAPACK;
import org.netlib.util.intW;

/**
 * This class provides basic functionality for a Multivariate Gaussian (Normal) Distribution.
 */
public class MultivariateGaussian {

	private static final LAPACK LAPACK_INST = LAPACK.getInstance();
	private static final double EPSILON;

	static {
		double eps = 1.0;
		while ((1.0 + (eps / 2.0)) != 1.0) {
			eps /= 2.0;
		}
		EPSILON = eps;
	}

	private final DenseVector mean;
	private final DenseMatrix cov;

	private DenseMatrix rootSigmaInv;
	private double u;

	// data buffers for computing pdf
	private DenseVector delta;
	private DenseVector v;

	/**
	 * The constructor.
	 *
	 * @param mean The mean vector of the distribution.
	 * @param cov  The covariance matrix of the distribution.
	 */
	public MultivariateGaussian(DenseVector mean, DenseMatrix cov) {
		this.mean = mean;
		this.cov = cov;
		this.delta = DenseVector.zeros(mean.size());
		this.v = DenseVector.zeros(mean.size());
		calculateCovarianceConstants();
	}

	/**
	 * Returns density of this multivariate Gaussian at given point, x.
	 */
	public double pdf(Vector x) {
		return Math.exp(logpdf(x));
	}

	/**
	 * Returns the log-density of this multivariate Gaussian at given point, x.
	 */
	public double logpdf(Vector x) {
		int n = mean.size();
		System.arraycopy(mean.getData(), 0, delta.getData(), 0, n);
		BLAS.scal(-1.0, delta);
		if (x instanceof DenseVector) {
			BLAS.axpy(1., (DenseVector) x, delta);
		} else if (x instanceof SparseVector) {
			BLAS.axpy(1., (SparseVector) x, delta);
		}
		BLAS.gemv(1.0, rootSigmaInv, false, delta, 0., v);
		return u - 0.5 * BLAS.dot(v, v);
	}

	/**
	 * Compute distribution dependent constants.
	 *
	 * <p>rootSigmaInv = D^(-1/2)^ * U.t, where sigma = U * D * U.t .
	 * u = log((2*pi)^(-k/2)^ * det(sigma)^(-1/2)^) .
	 */
	private void calculateCovarianceConstants() {
		int n = this.mean.size();
		int lwork = 3 * n - 1;
		double[] matA = new double[n * n];
		double[] work = new double[lwork];
		double[] evs = new double[n];
		intW info = new intW(0);

		for (int i = 0; i < n; i++) {
			System.arraycopy(cov.getData(), i * n, matA, i * n, i + 1);
		}
		LAPACK_INST.dsyev("V", "U", n, matA, n, evs, work, lwork, info);

		double maxEv = -Double.MAX_VALUE;
		for (int i = 0; i < n; i++) {
			if (evs[i] > maxEv) {
				maxEv = evs[i];
			}
		}
		double tol = EPSILON * n * maxEv;

		// log(pseudo-determinant) is sum of the logs of all non-zero singular values
		double logPseudoDetSigma = 0.;
		for (double ev : evs) {
			if (ev > tol) {
				logPseudoDetSigma += Math.log(ev);
			}
		}

		this.rootSigmaInv = new DenseMatrix(n, n);
		for (int i = 0; i < n; i++) {
			double pinvS = evs[i] > tol ? Math.sqrt(1.0 / evs[i]) : 0.;
			for (int j = 0; j < n; j++) {
				this.rootSigmaInv.set(i, j, pinvS * matA[j * n + i]);
			}
		}
		this.u = -0.5 * (n * Math.log(2.0 * Math.PI) + logPseudoDetSigma);
	}
}
