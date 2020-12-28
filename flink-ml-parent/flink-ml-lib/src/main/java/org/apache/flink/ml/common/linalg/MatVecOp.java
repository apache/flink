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

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A utility class that provides operations over {@link DenseVector}, {@link SparseVector} and
 * {@link DenseMatrix}.
 */
public class MatVecOp {
    /** compute vec1 + vec2 . */
    public static Vector plus(Vector vec1, Vector vec2) {
        return vec1.plus(vec2);
    }

    /** compute vec1 - vec2 . */
    public static Vector minus(Vector vec1, Vector vec2) {
        return vec1.minus(vec2);
    }

    /** Compute vec1 \cdot vec2 . */
    public static double dot(Vector vec1, Vector vec2) {
        return vec1.dot(vec2);
    }

    /** Compute || vec1 - vec2 ||_1 . */
    public static double sumAbsDiff(Vector vec1, Vector vec2) {
        if (vec1 instanceof DenseVector) {
            if (vec2 instanceof DenseVector) {
                return MatVecOp.applySum(
                        (DenseVector) vec1, (DenseVector) vec2, (a, b) -> Math.abs(a - b));
            } else {
                return MatVecOp.applySum(
                        (DenseVector) vec1, (SparseVector) vec2, (a, b) -> Math.abs(a - b));
            }
        } else {
            if (vec2 instanceof DenseVector) {
                return MatVecOp.applySum(
                        (SparseVector) vec1, (DenseVector) vec2, (a, b) -> Math.abs(a - b));
            } else {
                return MatVecOp.applySum(
                        (SparseVector) vec1, (SparseVector) vec2, (a, b) -> Math.abs(a - b));
            }
        }
    }

    /** Compute || vec1 - vec2 ||_2^2 . */
    public static double sumSquaredDiff(Vector vec1, Vector vec2) {
        if (vec1 instanceof DenseVector) {
            if (vec2 instanceof DenseVector) {
                return MatVecOp.applySum(
                        (DenseVector) vec1, (DenseVector) vec2, (a, b) -> (a - b) * (a - b));
            } else {
                return MatVecOp.applySum(
                        (DenseVector) vec1, (SparseVector) vec2, (a, b) -> (a - b) * (a - b));
            }
        } else {
            if (vec2 instanceof DenseVector) {
                return MatVecOp.applySum(
                        (SparseVector) vec1, (DenseVector) vec2, (a, b) -> (a - b) * (a - b));
            } else {
                return MatVecOp.applySum(
                        (SparseVector) vec1, (SparseVector) vec2, (a, b) -> (a - b) * (a - b));
            }
        }
    }

    /** y = func(x). */
    public static void apply(DenseMatrix x, DenseMatrix y, Function<Double, Double> func) {
        assert (x.m == y.m && x.n == y.n) : "x and y size mismatched.";
        double[] xdata = x.data;
        double[] ydata = y.data;
        for (int i = 0; i < xdata.length; i++) {
            ydata[i] = func.apply(xdata[i]);
        }
    }

    /** y = func(x1, x2). */
    public static void apply(
            DenseMatrix x1,
            DenseMatrix x2,
            DenseMatrix y,
            BiFunction<Double, Double, Double> func) {

        assert (x1.m == y.m && x1.n == y.n) : "x1 and y size mismatched.";
        assert (x2.m == y.m && x2.n == y.n) : "x2 and y size mismatched.";
        double[] x1data = x1.data;
        double[] x2data = x2.data;
        double[] ydata = y.data;
        for (int i = 0; i < ydata.length; i++) {
            ydata[i] = func.apply(x1data[i], x2data[i]);
        }
    }

    /** y = func(x). */
    public static void apply(DenseVector x, DenseVector y, Function<Double, Double> func) {
        assert (x.data.length == y.data.length) : "x and y size mismatched.";
        for (int i = 0; i < x.data.length; i++) {
            y.data[i] = func.apply(x.data[i]);
        }
    }

    /** y = func(x1, x2). */
    public static void apply(
            DenseVector x1,
            DenseVector x2,
            DenseVector y,
            BiFunction<Double, Double, Double> func) {

        assert (x1.data.length == y.data.length) : "x1 and y size mismatched.";
        assert (x2.data.length == y.data.length) : "x1 and y size mismatched.";
        for (int i = 0; i < y.data.length; i++) {
            y.data[i] = func.apply(x1.data[i], x2.data[i]);
        }
    }

    /**
     * Create a new {@link SparseVector} by element wise operation between two {@link
     * SparseVector}s. y = func(x1, x2).
     */
    public static SparseVector apply(
            SparseVector x1, SparseVector x2, BiFunction<Double, Double, Double> func) {
        assert (x1.size() == x2.size()) : "x1 and x2 size mismatched.";

        int totNnz = x1.values.length + x2.values.length;
        int p0 = 0;
        int p1 = 0;
        while (p0 < x1.values.length && p1 < x2.values.length) {
            if (x1.indices[p0] == x2.indices[p1]) {
                totNnz--;
                p0++;
                p1++;
            } else if (x1.indices[p0] < x2.indices[p1]) {
                p0++;
            } else {
                p1++;
            }
        }

        SparseVector r = new SparseVector(x1.size());
        r.indices = new int[totNnz];
        r.values = new double[totNnz];
        p0 = p1 = 0;
        int pos = 0;
        while (pos < totNnz) {
            if (p0 < x1.values.length && p1 < x2.values.length) {
                if (x1.indices[p0] == x2.indices[p1]) {
                    r.indices[pos] = x1.indices[p0];
                    r.values[pos] = func.apply(x1.values[p0], x2.values[p1]);
                    p0++;
                    p1++;
                } else if (x1.indices[p0] < x2.indices[p1]) {
                    r.indices[pos] = x1.indices[p0];
                    r.values[pos] = func.apply(x1.values[p0], 0.0);
                    p0++;
                } else {
                    r.indices[pos] = x2.indices[p1];
                    r.values[pos] = func.apply(0.0, x2.values[p1]);
                    p1++;
                }
                pos++;
            } else {
                if (p0 < x1.values.length) {
                    r.indices[pos] = x1.indices[p0];
                    r.values[pos] = func.apply(x1.values[p0], 0.0);
                    p0++;
                    pos++;
                    continue;
                }
                if (p1 < x2.values.length) {
                    r.indices[pos] = x2.indices[p1];
                    r.values[pos] = func.apply(0.0, x2.values[p1]);
                    p1++;
                    pos++;
                    continue;
                }
            }
        }

        return r;
    }

    /** \sum_i func(x1_i, x2_i) . */
    public static double applySum(
            DenseVector x1, DenseVector x2, BiFunction<Double, Double, Double> func) {
        assert x1.size() == x2.size() : "x1 and x2 size mismatched.";
        double[] x1data = x1.getData();
        double[] x2data = x2.getData();
        double s = 0.;
        for (int i = 0; i < x1data.length; i++) {
            s += func.apply(x1data[i], x2data[i]);
        }
        return s;
    }

    /** \sum_i func(x1_i, x2_i) . */
    public static double applySum(
            SparseVector x1, SparseVector x2, BiFunction<Double, Double, Double> func) {
        double s = 0.;
        int p1 = 0;
        int p2 = 0;
        int[] x1Indices = x1.getIndices();
        double[] x1Values = x1.getValues();
        int[] x2Indices = x2.getIndices();
        double[] x2Values = x2.getValues();
        int nnz1 = x1Indices.length;
        int nnz2 = x2Indices.length;
        while (p1 < nnz1 || p2 < nnz2) {
            if (p1 < nnz1 && p2 < nnz2) {
                if (x1Indices[p1] == x2Indices[p2]) {
                    s += func.apply(x1Values[p1], x2Values[p2]);
                    p1++;
                    p2++;
                } else if (x1Indices[p1] < x2Indices[p2]) {
                    s += func.apply(x1Values[p1], 0.);
                    p1++;
                } else {
                    s += func.apply(0., x2Values[p2]);
                    p2++;
                }
            } else {
                if (p1 < nnz1) {
                    s += func.apply(x1Values[p1], 0.);
                    p1++;
                } else { // p2 < nnz2
                    s += func.apply(0., x2Values[p2]);
                    p2++;
                }
            }
        }
        return s;
    }

    /** \sum_i func(x1_i, x2_i) . */
    public static double applySum(
            DenseVector x1, SparseVector x2, BiFunction<Double, Double, Double> func) {
        assert x1.size() == x2.size() : "x1 and x2 size mismatched.";
        double s = 0.;
        int p2 = 0;
        int[] x2Indices = x2.getIndices();
        double[] x2Values = x2.getValues();
        int nnz2 = x2Indices.length;
        double[] x1data = x1.getData();
        for (int i = 0; i < x1data.length; i++) {
            if (p2 < nnz2 && x2Indices[p2] == i) {
                s += func.apply(x1data[i], x2Values[p2]);
                p2++;
            } else {
                s += func.apply(x1data[i], 0.);
            }
        }
        return s;
    }

    /** \sum_i func(x1_i, x2_i) . */
    public static double applySum(
            SparseVector x1, DenseVector x2, BiFunction<Double, Double, Double> func) {
        assert x1.size() == x2.size() : "x1 and x2 size mismatched.";
        double s = 0.;
        int p1 = 0;
        int[] x1Indices = x1.getIndices();
        double[] x1Values = x1.getValues();
        int nnz1 = x1Indices.length;
        double[] x2data = x2.getData();
        for (int i = 0; i < x2data.length; i++) {
            if (p1 < nnz1 && x1Indices[p1] == i) {
                s += func.apply(x1Values[p1], x2data[i]);
                p1++;
            } else {
                s += func.apply(0., x2data[i]);
            }
        }
        return s;
    }

    /** \sum_ij func(x1_ij, x2_ij) . */
    public static double applySum(
            DenseMatrix x1, DenseMatrix x2, BiFunction<Double, Double, Double> func) {
        assert (x1.m == x2.m && x1.n == x2.n) : "x1 and x2 size mismatched.";
        double[] x1data = x1.data;
        double[] x2data = x2.data;
        double s = 0.;
        for (int i = 0; i < x1data.length; i++) {
            s += func.apply(x1data[i], x2data[i]);
        }
        return s;
    }
}
