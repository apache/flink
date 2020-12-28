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

import org.apache.flink.shaded.guava18.com.google.common.primitives.Doubles;

import com.github.fommil.netlib.LAPACK;
import org.netlib.util.intW;

/** This class provides basic functionality for a Multivariate Gaussian (Normal) Distribution. */
public class MultivariateGaussian {

    private static final LAPACK LAPACK_INST = LAPACK.getInstance();
    private static final com.github.fommil.netlib.BLAS F2J_BLAS_INST =
            com.github.fommil.netlib.F2jBLAS.getInstance();
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
     * @param cov The covariance matrix of the distribution.
     */
    public MultivariateGaussian(DenseVector mean, DenseMatrix cov) {
        this.mean = mean;
        this.cov = cov;
        this.delta = DenseVector.zeros(mean.size());
        this.v = DenseVector.zeros(mean.size());
        calculateCovarianceConstants();
    }

    /** Returns density of this multivariate Gaussian at given point x . */
    public double pdf(Vector x) {
        return Math.exp(logpdf(x));
    }

    /** Returns the log-density of this multivariate Gaussian at given point x . */
    public double logpdf(Vector x) {
        int n = mean.size();
        System.arraycopy(mean.getData(), 0, delta.getData(), 0, n);
        BLAS.scal(-1.0, delta);
        if (x instanceof DenseVector) {
            BLAS.axpy(1., (DenseVector) x, delta);
        } else if (x instanceof SparseVector) {
            BLAS.axpy(1., (SparseVector) x, delta);
        }
        BLAS.gemv(1.0, rootSigmaInv, true, delta, 0., v);
        return u - 0.5 * BLAS.dot(v, v);
    }

    /**
     * Compute distribution dependent constants.
     *
     * <p>The probability density function is calculated as: pdf(x) = (2*pi)^(-k/2)^ *
     * det(sigma)^(-1/2)^ * exp((-1/2) * (x-mu).t * inv(sigma) * (x-mu))
     *
     * <p>Here we compute the following distribution dependent constants that can be reused in each
     * pdf computation: A) u = log((2*pi)^(-k/2)^ * det(sigma)^(-1/2)^) B) rootSigmaInv =
     * sqrt(inv(sigma)) = U * D^(-1/2)^
     *
     * <ul>
     *   <li>sigma = U * D * U.t
     *   <li>inv(sigma) = U * inv(D) * U.t = (U * D^(-1/2)^) * (U * D^(-1/2)^).t
     *   <li>sqrt(inv(sigma)) = U * D^(-1/2)^
     * </ul>
     */
    private void calculateCovarianceConstants() {
        int k = this.mean.size();
        int lwork = 3 * k - 1;
        double[] matU = new double[k * k];
        double[] work = new double[lwork];
        double[] evs = new double[k];
        intW info = new intW(0);

        System.arraycopy(cov.getData(), 0, matU, 0, k * k);
        LAPACK_INST.dsyev("V", "U", k, matU, k, evs, work, lwork, info);

        double maxEv = Doubles.max(evs);
        double tol = EPSILON * k * maxEv;

        // log(pseudo-determinant) is sum of the logs of all non-zero singular values
        double logPseudoDetSigma = 0.;
        for (double ev : evs) {
            if (ev > tol) {
                logPseudoDetSigma += Math.log(ev);
            }
        }

        for (int i = 0; i < k; i++) {
            double invEv = evs[i] > tol ? Math.sqrt(1.0 / evs[i]) : 0.;
            F2J_BLAS_INST.dscal(k, invEv, matU, i * k, 1);
        }
        this.rootSigmaInv = new DenseMatrix(k, k, matU);
        this.u = -0.5 * (k * Math.log(2.0 * Math.PI) + logPseudoDetSigma);
    }
}
