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

import com.github.fommil.netlib.BLAS;
import com.github.fommil.netlib.LAPACK;
import org.netlib.util.intW;

import java.io.Serializable;

/**
 * Multivariate Gaussian.
 */
public class MultivariateGaussian implements Serializable {

	private static final BLAS blas = BLAS.getInstance();
	private static final LAPACK lapack = LAPACK.getInstance();

	private double[] mean;
	private double[][] cov;

	private transient double[] rootSigmaInv;
	private transient double u;

	// data buffers for computing logpdf
	private transient double[] delta;
	private transient double[] v;

	public MultivariateGaussian(double[] mean, double[][] cov) {
		this.mean = mean;
		this.cov = cov;

		this.delta = new double[mean.length];
		this.v = new double[mean.length];

		/**
		 * Compute distribution dependent constants:
		 *    rootSigmaInv = D^(-1/2)^ * U.t, where sigma = U * D * U.t
		 *    u = log((2*pi)^(-k/2)^ * det(sigma)^(-1/2)^)
		 */
		calculateCovarianceConstants();
	}

	/**
	 * Calculate distribution dependent components used for the density function:
	 * pdf(x) = (2*pi)^(-k/2)^ * det(sigma)^(-1/2)^ * exp((-1/2) * (x-mu).t * inv(sigma) * (x-mu))
	 * where k is length of the mean vector.
	 * We here compute distribution-fixed parts
	 * log((2*pi)^(-k/2)^ * det(sigma)^(-1/2)^)
	 * and
	 * D^(-1/2)^ * U, where sigma = U * D * U.t
	 * Both the determinant and the inverse can be computed from the singular value decomposition
	 * of sigma.  Noting that covariance matrices are always symmetric and positive semi-definite,
	 * we can use the eigendecomposition. We also do not compute the inverse directly; noting
	 * that
	 * sigma = U * D * U.t
	 * inv(Sigma) = U * inv(D) * U.t
	 * = (D^{-1/2}^ * U.t).t * (D^{-1/2}^ * U.t)
	 * and thus
	 * -0.5 * (x-mu).t * inv(Sigma) * (x-mu) = -0.5 * norm(D^{-1/2}^ * U.t  * (x-mu))^2^
	 * To guard against singular covariance matrices, this method computes both the
	 * pseudo-determinant and the pseudo-inverse (Moore-Penrose).  Singular values are considered
	 * to be non-zero only if they exceed a tolerance based on machine precision, matrix size, and
	 * relation to the maximum singular value (same tolerance used by, e.g., Octave).
	 */
	private void calculateCovarianceConstants() {
		int n = this.mean.length;
		int lwork = 3 * n - 1;
		double[] matA = new double[n * n];
		double[] work = new double[lwork];
		double[] evs = new double[n];
		intW info = new intW(0);

		for (int i = 0; i < n; i++) {
			for (int j = 0; j <= i; j++) {
				matA[i * n + j] = this.cov[i][j];
			}
		}

		lapack.dsyev("V", "L", n, matA, n, evs, work, lwork, info);

		double maxEv = 0.;
		for (int i = 0; i < evs.length; i++) {
			if (evs[i] > maxEv) {
				maxEv = evs[i];
			}
		}
		double tol = epsilon * n * maxEv;

		// log(pseudo-determinant) is sum of the logs of all non-zero singular values
		double logPseudoDetSigma = 0.;
		for (int i = 0; i < evs.length; i++) {
			if (evs[i] > tol) {
				logPseudoDetSigma += Math.log(evs[i]);
			}
		}

		this.rootSigmaInv = new double[n * n];
		for (int i = 0; i < n; i++) {
			double pinvS = evs[i] > tol ? Math.sqrt(1.0 / evs[i]) : 0.;
			for (int j = 0; j < n; j++) {
				this.rootSigmaInv[i * n + j] = pinvS * matA[j * n + i];
			}
		}

		this.u = -0.5 * (n * Math.log(2.0 * Math.PI) + logPseudoDetSigma);
	}

	public double pdf(double[] x) {
		return Math.exp(logpdf(x));
	}

	public double logpdf(double[] x) {
		int n = mean.length;
		System.arraycopy(x, 0, delta, 0, n);
		blas.daxpy(n, -1., mean, 1, delta, 1);
		blas.dgemv("N", n, n, 1., rootSigmaInv, n, delta, 1, 0., v, 1);
		return u - 0.5 * blas.ddot(n, v, 1, v, 1);
	}

	public static double epsilon;

	static {
		double eps = 1.0;
		while ((1.0 + (eps / 2.0)) != 1.0) {
			eps /= 2.0;
		}
		epsilon = eps;
	}
}
