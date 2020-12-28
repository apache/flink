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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** A sparse vector represented by an indices array and a values array. */
public class SparseVector extends Vector {

    /**
     * Size of the vector. n = -1 indicates that the vector size is undetermined.
     *
     * <p>Package private to allow access from {@link MatVecOp} and {@link BLAS}.
     */
    int n;

    /**
     * Column indices.
     *
     * <p>Package private to allow access from {@link MatVecOp} and {@link BLAS}.
     */
    int[] indices;

    /**
     * Column values.
     *
     * <p>Package private to allow access from {@link MatVecOp} and {@link BLAS}.
     */
    double[] values;

    /** Construct an empty sparse vector with undetermined size. */
    public SparseVector() {
        this(-1);
    }

    /** Construct an empty sparse vector with determined size. */
    public SparseVector(int n) {
        this.n = n;
        this.indices = new int[0];
        this.values = new double[0];
    }

    /**
     * Construct a sparse vector with the given indices and values.
     *
     * @throws IllegalArgumentException If size of indices array and values array differ.
     * @throws IllegalArgumentException If n >= 0 and the indices are out of bound.
     */
    public SparseVector(int n, int[] indices, double[] values) {
        this.n = n;
        this.indices = indices;
        this.values = values;
        checkSizeAndIndicesRange();
        sortIndices();
    }

    /**
     * Construct a sparse vector with given indices to values map.
     *
     * @throws IllegalArgumentException If n >= 0 and the indices are out of bound.
     */
    public SparseVector(int n, Map<Integer, Double> kv) {
        this.n = n;
        int nnz = kv.size();
        int[] indices = new int[nnz];
        double[] values = new double[nnz];

        int pos = 0;
        for (Map.Entry<Integer, Double> entry : kv.entrySet()) {
            indices[pos] = entry.getKey();
            values[pos] = entry.getValue();
            pos++;
        }

        this.indices = indices;
        this.values = values;
        checkSizeAndIndicesRange();

        if (!(kv instanceof TreeMap)) {
            sortIndices();
        }
    }

    /**
     * Check whether the indices array and values array are of the same size, and whether vector
     * indices are in valid range.
     */
    private void checkSizeAndIndicesRange() {
        if (indices.length != values.length) {
            throw new IllegalArgumentException("Indices size and values size should be the same.");
        }
        for (int i = 0; i < indices.length; i++) {
            if (indices[i] < 0 || (n >= 0 && indices[i] >= n)) {
                throw new IllegalArgumentException("Index out of bound.");
            }
        }
    }

    /** Sort the indices and values using quick sort. */
    private static void sortImpl(int[] indices, double[] values, int low, int high) {
        int pivot = indices[high];
        int pos = low - 1;
        for (int i = low; i <= high; i++) {
            if (indices[i] <= pivot) {
                pos++;
                int tempI = indices[pos];
                double tempD = values[pos];
                indices[pos] = indices[i];
                values[pos] = values[i];
                indices[i] = tempI;
                values[i] = tempD;
            }
        }
        if (pos - 1 > low) {
            sortImpl(indices, values, low, pos - 1);
        }
        if (high > pos + 1) {
            sortImpl(indices, values, pos + 1, high);
        }
    }

    /** Sort the indices and values if the indices are out of order. */
    private void sortIndices() {
        boolean outOfOrder = false;
        for (int i = 0; i < this.indices.length - 1; i++) {
            if (this.indices[i] > this.indices[i + 1]) {
                outOfOrder = true;
                break;
            }
        }
        if (outOfOrder) {
            sortImpl(this.indices, this.values, 0, this.indices.length - 1);
        }
    }

    @Override
    public SparseVector clone() {
        SparseVector vec = new SparseVector(this.n);
        vec.indices = this.indices.clone();
        vec.values = this.values.clone();
        return vec;
    }

    @Override
    public SparseVector prefix(double d) {
        int[] indices = new int[this.indices.length + 1];
        double[] values = new double[this.values.length + 1];
        int n = (this.n >= 0) ? this.n + 1 : this.n;

        indices[0] = 0;
        values[0] = d;

        for (int i = 0; i < this.indices.length; i++) {
            indices[i + 1] = this.indices[i] + 1;
            values[i + 1] = this.values[i];
        }

        return new SparseVector(n, indices, values);
    }

    @Override
    public SparseVector append(double d) {
        int[] indices = new int[this.indices.length + 1];
        double[] values = new double[this.values.length + 1];
        int n = (this.n >= 0) ? this.n + 1 : this.n;

        System.arraycopy(this.indices, 0, indices, 0, this.indices.length);
        System.arraycopy(this.values, 0, values, 0, this.values.length);

        indices[this.indices.length] = n - 1;
        values[this.values.length] = d;

        return new SparseVector(n, indices, values);
    }

    /** Get the indices array. */
    public int[] getIndices() {
        return indices;
    }

    /** Get the values array. */
    public double[] getValues() {
        return values;
    }

    @Override
    public int size() {
        return n;
    }

    @Override
    public double get(int i) {
        int pos = Arrays.binarySearch(indices, i);
        if (pos >= 0) {
            return values[pos];
        }
        return 0.;
    }

    /** Set the size of the vector. */
    public void setSize(int n) {
        this.n = n;
    }

    /** Get number of values in this vector. */
    public int numberOfValues() {
        return this.values.length;
    }

    @Override
    public void set(int i, double val) {
        int pos = Arrays.binarySearch(indices, i);
        if (pos >= 0) {
            this.values[pos] = val;
        } else {
            pos = -(pos + 1);
            insert(pos, i, val);
        }
    }

    @Override
    public void add(int i, double val) {
        int pos = Arrays.binarySearch(indices, i);
        if (pos >= 0) {
            this.values[pos] += val;
        } else {
            pos = -(pos + 1);
            insert(pos, i, val);
        }
    }

    /** Insert value "val" in the position "pos" with index "index". */
    private void insert(int pos, int index, double val) {
        double[] newValues = new double[this.values.length + 1];
        int[] newIndices = new int[this.values.length + 1];
        System.arraycopy(this.values, 0, newValues, 0, pos);
        System.arraycopy(this.indices, 0, newIndices, 0, pos);
        newValues[pos] = val;
        newIndices[pos] = index;
        System.arraycopy(this.values, pos, newValues, pos + 1, this.values.length - pos);
        System.arraycopy(this.indices, pos, newIndices, pos + 1, this.values.length - pos);
        this.values = newValues;
        this.indices = newIndices;
    }

    @Override
    public String toString() {
        return VectorUtil.toString(this);
    }

    @Override
    public double normL2() {
        double d = 0;
        for (double t : values) {
            d += t * t;
        }
        return Math.sqrt(d);
    }

    @Override
    public double normL1() {
        double d = 0;
        for (double t : values) {
            d += Math.abs(t);
        }
        return d;
    }

    @Override
    public double normInf() {
        double d = 0;
        for (double t : values) {
            d = Math.max(Math.abs(t), d);
        }
        return d;
    }

    @Override
    public double normL2Square() {
        double d = 0;
        for (double t : values) {
            d += t * t;
        }
        return d;
    }

    @Override
    public SparseVector slice(int[] indices) {
        SparseVector sliced = new SparseVector(indices.length);
        int nnz = 0;
        sliced.indices = new int[indices.length];
        sliced.values = new double[indices.length];

        for (int i = 0; i < indices.length; i++) {
            int pos = Arrays.binarySearch(this.indices, indices[i]);
            if (pos >= 0) {
                sliced.indices[nnz] = i;
                sliced.values[nnz] = this.values[pos];
                nnz++;
            }
        }

        if (nnz < sliced.indices.length) {
            sliced.indices = Arrays.copyOf(sliced.indices, nnz);
            sliced.values = Arrays.copyOf(sliced.values, nnz);
        }

        return sliced;
    }

    @Override
    public Vector plus(Vector vec) {
        if (this.size() != vec.size()) {
            throw new IllegalArgumentException("The size of the two vectors are different.");
        }

        if (vec instanceof DenseVector) {
            DenseVector r = ((DenseVector) vec).clone();
            for (int i = 0; i < this.indices.length; i++) {
                r.add(this.indices[i], this.values[i]);
            }
            return r;
        } else {
            return MatVecOp.apply(this, (SparseVector) vec, ((a, b) -> a + b));
        }
    }

    @Override
    public Vector minus(Vector vec) {
        if (this.size() != vec.size()) {
            throw new IllegalArgumentException("The size of the two vectors are different.");
        }

        if (vec instanceof DenseVector) {
            DenseVector r = ((DenseVector) vec).scale(-1.0);
            for (int i = 0; i < this.indices.length; i++) {
                r.add(this.indices[i], this.values[i]);
            }
            return r;
        } else {
            return MatVecOp.apply(this, (SparseVector) vec, ((a, b) -> a - b));
        }
    }

    @Override
    public SparseVector scale(double d) {
        SparseVector r = this.clone();
        BLAS.scal(d, r);
        return r;
    }

    @Override
    public void scaleEqual(double d) {
        BLAS.scal(d, this);
    }

    /** Remove all zero values away from this vector. */
    public void removeZeroValues() {
        if (this.values.length != 0) {
            List<Integer> idxs = new ArrayList<>();
            for (int i = 0; i < values.length; i++) {
                if (0 != values[i]) {
                    idxs.add(i);
                }
            }
            int[] newIndices = new int[idxs.size()];
            double[] newValues = new double[newIndices.length];
            for (int i = 0; i < newIndices.length; i++) {
                newIndices[i] = indices[idxs.get(i)];
                newValues[i] = values[idxs.get(i)];
            }
            this.indices = newIndices;
            this.values = newValues;
        }
    }

    private double dot(SparseVector other) {
        if (this.size() != other.size()) {
            throw new RuntimeException("the size of the two vectors are different");
        }

        double d = 0;
        int p0 = 0;
        int p1 = 0;
        while (p0 < this.values.length && p1 < other.values.length) {
            if (this.indices[p0] == other.indices[p1]) {
                d += this.values[p0] * other.values[p1];
                p0++;
                p1++;
            } else if (this.indices[p0] < other.indices[p1]) {
                p0++;
            } else {
                p1++;
            }
        }
        return d;
    }

    private double dot(DenseVector other) {
        if (this.size() != other.size()) {
            throw new RuntimeException(
                    "The size of the two vectors are different: "
                            + this.size()
                            + " vs "
                            + other.size());
        }
        double s = 0.;
        for (int i = 0; i < this.indices.length; i++) {
            s += this.values[i] * other.get(this.indices[i]);
        }
        return s;
    }

    @Override
    public double dot(Vector other) {
        if (other instanceof DenseVector) {
            return dot((DenseVector) other);
        } else {
            return dot((SparseVector) other);
        }
    }

    @Override
    public DenseMatrix outer() {
        return this.outer(this);
    }

    /**
     * Compute the outer product with another vector.
     *
     * @return The outer product matrix.
     */
    public DenseMatrix outer(SparseVector other) {
        int nrows = this.size();
        int ncols = other.size();
        double[] data = new double[ncols * nrows];
        for (int i = 0; i < this.values.length; i++) {
            for (int j = 0; j < other.values.length; j++) {
                data[this.indices[i] + other.indices[j] * nrows] = this.values[i] * other.values[j];
            }
        }
        return new DenseMatrix(nrows, ncols, data);
    }

    /** Convert to a dense vector. */
    public DenseVector toDenseVector() {
        if (n >= 0) {
            DenseVector r = new DenseVector(n);
            for (int i = 0; i < this.indices.length; i++) {
                r.set(this.indices[i], this.values[i]);
            }
            return r;
        } else {
            if (this.indices.length == 0) {
                return new DenseVector();
            } else {
                int n = this.indices[this.indices.length - 1] + 1;
                DenseVector r = new DenseVector(n);
                for (int i = 0; i < this.indices.length; i++) {
                    r.set(this.indices[i], this.values[i]);
                }
                return r;
            }
        }
    }

    @Override
    public void standardizeEqual(double mean, double stdvar) {
        for (int i = 0; i < indices.length; i++) {
            values[i] -= mean;
            values[i] *= (1.0 / stdvar);
        }
    }

    @Override
    public void normalizeEqual(double p) {
        double norm = 0.0;
        if (Double.isInfinite(p)) {
            norm = normInf();
        } else if (p == 1.0) {
            norm = normL1();
        } else if (p == 2.0) {
            norm = normL2();
        } else {
            for (int i = 0; i < indices.length; i++) {
                norm += Math.pow(values[i], p);
            }
            norm = Math.pow(norm, 1 / p);
        }

        for (int i = 0; i < indices.length; i++) {
            values[i] /= norm;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SparseVector that = (SparseVector) o;
        return n == that.n
                && Arrays.equals(indices, that.indices)
                && Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(n);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    public VectorIterator iterator() {
        return new SparseVectorVectorIterator();
    }

    private class SparseVectorVectorIterator implements VectorIterator {
        private int cursor = 0;

        @Override
        public boolean hasNext() {
            return cursor < values.length;
        }

        @Override
        public void next() {
            cursor++;
        }

        @Override
        public int getIndex() {
            if (cursor >= values.length) {
                throw new RuntimeException("Iterator out of bound.");
            }
            return indices[cursor];
        }

        @Override
        public double getValue() {
            if (cursor >= values.length) {
                throw new RuntimeException("Iterator out of bound.");
            }
            return values[cursor];
        }
    }
}
