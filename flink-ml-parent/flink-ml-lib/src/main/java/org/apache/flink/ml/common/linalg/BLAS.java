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

package org.apache.flink.ml.common.linalg;

import org.apache.flink.util.Preconditions;

/** A utility class that provides BLAS routines over matrices and vectors. */
public class BLAS {

    /** For level-1 routines, we use Java implementation. */
    private static final com.github.fommil.netlib.BLAS NATIVE_BLAS =
            com.github.fommil.netlib.BLAS.getInstance();

    /**
     * For level-2 and level-3 routines, we use the native BLAS.
     *
     * <p>The NATIVE_BLAS instance tries to load BLAS implementations in the order: 1) optimized
     * system libraries such as Intel MKL, 2) self-contained native builds using the reference
     * Fortran from netlib.org, 3) F2J implementation. If to use optimized system libraries, it is
     * important to turn of their multi-thread support. Otherwise, it will conflict with Flink's
     * executor and leads to performance loss.
     */
    private static final com.github.fommil.netlib.BLAS F2J_BLAS =
            com.github.fommil.netlib.F2jBLAS.getInstance();

    /** \sum_i |x_i| . */
    public static double asum(int n, double[] x, int offset) {
        return F2J_BLAS.dasum(n, x, offset, 1);
    }

    /** \sum_i |x_i| . */
    public static double asum(DenseVector x) {
        return asum(x.data.length, x.data, 0);
    }

    /** \sum_i |x_i| . */
    public static double asum(SparseVector x) {
        return asum(x.values.length, x.values, 0);
    }

    /** y += a * x . */
    public static void axpy(double a, double[] x, double[] y) {
        Preconditions.checkArgument(x.length == y.length, "Array dimension mismatched.");
        F2J_BLAS.daxpy(x.length, a, x, 1, y, 1);
    }

    /** y += a * x . */
    public static void axpy(double a, DenseVector x, DenseVector y) {
        Preconditions.checkArgument(x.data.length == y.data.length, "Vector dimension mismatched.");
        F2J_BLAS.daxpy(x.data.length, a, x.data, 1, y.data, 1);
    }

    /** y += a * x . */
    public static void axpy(double a, SparseVector x, DenseVector y) {
        for (int i = 0; i < x.indices.length; i++) {
            y.data[x.indices[i]] += a * x.values[i];
        }
    }

    /** y += a * x . */
    public static void axpy(double a, DenseMatrix x, DenseMatrix y) {
        Preconditions.checkArgument(x.m == y.m && x.n == y.n, "Matrix dimension mismatched.");
        F2J_BLAS.daxpy(x.data.length, a, x.data, 1, y.data, 1);
    }

    /** y[yOffset:yOffset+n] += a * x[xOffset:xOffset+n] . */
    public static void axpy(int n, double a, double[] x, int xOffset, double[] y, int yOffset) {
        F2J_BLAS.daxpy(n, a, x, xOffset, 1, y, yOffset, 1);
    }

    /** x \cdot y . */
    public static double dot(double[] x, double[] y) {
        Preconditions.checkArgument(x.length == y.length, "Array dimension mismatched.");
        double s = 0.;
        for (int i = 0; i < x.length; i++) {
            s += x[i] * y[i];
        }
        return s;
    }

    /** x \cdot y . */
    public static double dot(DenseVector x, DenseVector y) {
        return dot(x.getData(), y.getData());
    }

    /** x = x * a . */
    public static void scal(double a, double[] x) {
        F2J_BLAS.dscal(x.length, a, x, 1);
    }

    /** x = x * a . */
    public static void scal(double a, DenseVector x) {
        F2J_BLAS.dscal(x.data.length, a, x.data, 1);
    }

    /** x = x * a . */
    public static void scal(double a, SparseVector x) {
        F2J_BLAS.dscal(x.values.length, a, x.values, 1);
    }

    /** x = x * a . */
    public static void scal(double a, DenseMatrix x) {
        F2J_BLAS.dscal(x.data.length, a, x.data, 1);
    }

    /** C := alpha * A * B + beta * C . */
    public static void gemm(
            double alpha,
            DenseMatrix matA,
            boolean transA,
            DenseMatrix matB,
            boolean transB,
            double beta,
            DenseMatrix matC) {
        if (transA) {
            Preconditions.checkArgument(
                    matA.numCols() == matC.numRows(),
                    "The columns of A does not match the rows of C");
        } else {
            Preconditions.checkArgument(
                    matA.numRows() == matC.numRows(), "The rows of A does not match the rows of C");
        }
        if (transB) {
            Preconditions.checkArgument(
                    matB.numRows() == matC.numCols(),
                    "The rows of B does not match the columns of C");
        } else {
            Preconditions.checkArgument(
                    matB.numCols() == matC.numCols(),
                    "The columns of B does not match the columns of C");
        }

        final int m = matC.numRows();
        final int n = matC.numCols();
        final int k = transA ? matA.numRows() : matA.numCols();
        final int lda = matA.numRows();
        final int ldb = matB.numRows();
        final int ldc = matC.numRows();
        final String ta = transA ? "T" : "N";
        final String tb = transB ? "T" : "N";
        NATIVE_BLAS.dgemm(
                ta,
                tb,
                m,
                n,
                k,
                alpha,
                matA.getData(),
                lda,
                matB.getData(),
                ldb,
                beta,
                matC.getData(),
                ldc);
    }

    /** Check the compatibility of matrix and vector sizes in <code>gemv</code>. */
    private static void gemvDimensionCheck(DenseMatrix matA, boolean transA, Vector x, Vector y) {
        if (transA) {
            Preconditions.checkArgument(
                    matA.numCols() == y.size() && matA.numRows() == x.size(),
                    "Matrix and vector size mismatched.");
        } else {
            Preconditions.checkArgument(
                    matA.numRows() == y.size() && matA.numCols() == x.size(),
                    "Matrix and vector size mismatched.");
        }
    }

    /** y := alpha * A * x + beta * y . */
    public static void gemv(
            double alpha,
            DenseMatrix matA,
            boolean transA,
            DenseVector x,
            double beta,
            DenseVector y) {
        gemvDimensionCheck(matA, transA, x, y);
        final int m = matA.numRows();
        final int n = matA.numCols();
        final int lda = matA.numRows();
        final String ta = transA ? "T" : "N";
        NATIVE_BLAS.dgemv(
                ta, m, n, alpha, matA.getData(), lda, x.getData(), 1, beta, y.getData(), 1);
    }

    /** y := alpha * A * x + beta * y . */
    public static void gemv(
            double alpha,
            DenseMatrix matA,
            boolean transA,
            SparseVector x,
            double beta,
            DenseVector y) {
        gemvDimensionCheck(matA, transA, x, y);
        final int m = matA.numRows();
        final int n = matA.numCols();
        if (transA) {
            int start = 0;
            for (int i = 0; i < n; i++) {
                double s = 0.;
                for (int j = 0; j < x.indices.length; j++) {
                    s += x.values[j] * matA.data[start + x.indices[j]];
                }
                y.data[i] = beta * y.data[i] + alpha * s;
                start += m;
            }
        } else {
            scal(beta, y);
            for (int i = 0; i < x.indices.length; i++) {
                int index = x.indices[i];
                double value = alpha * x.values[i];
                F2J_BLAS.daxpy(m, value, matA.data, index * m, 1, y.data, 0, 1);
            }
        }
    }
}
