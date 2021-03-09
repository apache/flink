/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.util;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;

/**
 * Implements a feature vector as a multi-dimensional point. Coordinates of that point (= the
 * features) are stored as double values. The distance between two feature vectors is the Euclidean
 * distance between the points.
 */
public final class CoordVector implements Value, Comparable<CoordVector> {
    private static final long serialVersionUID = 1L;

    // coordinate array
    private double[] coordinates;

    /** Initializes a blank coordinate vector. Required for deserialization! */
    public CoordVector() {
        coordinates = null;
    }

    /**
     * Initializes a coordinate vector.
     *
     * @param coordinates The coordinate vector of a multi-dimensional point.
     */
    public CoordVector(Double[] coordinates) {
        this.coordinates = new double[coordinates.length];
        for (int i = 0; i < coordinates.length; i++) {
            this.coordinates[i] = coordinates[i];
        }
    }

    /**
     * Initializes a coordinate vector.
     *
     * @param coordinates The coordinate vector of a multi-dimensional point.
     */
    public CoordVector(double[] coordinates) {
        this.coordinates = coordinates;
    }

    /**
     * Returns the coordinate vector of a multi-dimensional point.
     *
     * @return The coordinate vector of a multi-dimensional point.
     */
    public double[] getCoordinates() {
        return this.coordinates;
    }

    /**
     * Sets the coordinate vector of a multi-dimensional point.
     *
     * @param coordinates The dimension values of the point.
     */
    public void setCoordinates(double[] coordinates) {
        this.coordinates = coordinates;
    }

    /**
     * Computes the Euclidean distance between this coordinate vector and a second coordinate
     * vector.
     *
     * @param cv The coordinate vector to which the distance is computed.
     * @return The Euclidean distance to coordinate vector cv. If cv has a different length than
     *     this coordinate vector, -1 is returned.
     */
    public double computeEuclideanDistance(CoordVector cv) {
        // check coordinate vector lengths
        if (cv.coordinates.length != this.coordinates.length) {
            return -1.0;
        }

        double quadSum = 0.0;
        for (int i = 0; i < this.coordinates.length; i++) {
            double diff = this.coordinates[i] - cv.coordinates[i];
            quadSum += diff * diff;
        }
        return Math.sqrt(quadSum);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        int length = in.readInt();
        this.coordinates = new double[length];
        for (int i = 0; i < length; i++) {
            this.coordinates[i] = in.readDouble();
        }
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeInt(this.coordinates.length);
        for (int i = 0; i < this.coordinates.length; i++) {
            out.writeDouble(this.coordinates[i]);
        }
    }

    /**
     * Compares this coordinate vector to another key.
     *
     * @return -1 if the other key is not of type CoordVector. If the other key is also a
     *     CoordVector but its length differs from this coordinates vector, -1 is return if this
     *     coordinate vector is smaller and 1 if it is larger. If both coordinate vectors have the
     *     same length, the coordinates of both are compared. If a coordinate of this coordinate
     *     vector is smaller than the corresponding coordinate of the other vector -1 is returned
     *     and 1 otherwise. If all coordinates are identical 0 is returned.
     */
    @Override
    public int compareTo(CoordVector o) {
        // check if both coordinate vectors have identical lengths
        if (o.coordinates.length > this.coordinates.length) {
            return -1;
        } else if (o.coordinates.length < this.coordinates.length) {
            return 1;
        }

        // compare all coordinates
        for (int i = 0; i < this.coordinates.length; i++) {
            if (o.coordinates[i] > this.coordinates[i]) {
                return -1;
            } else if (o.coordinates[i] < this.coordinates[i]) {
                return 1;
            }
        }
        return 0;
    }
}
