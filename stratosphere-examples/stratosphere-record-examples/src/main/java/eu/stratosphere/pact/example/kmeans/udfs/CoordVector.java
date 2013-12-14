/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.example.kmeans.udfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.types.Key;

/**
 * Implements a feature vector as a multi-dimensional point. Coordinates of that point
 * (= the features) are stored as double values. The distance between two feature vectors is
 * the Euclidian distance between the points.
 */
public final class CoordVector implements Key {
	private static final long serialVersionUID = 1L;
	
	// coordinate array
	private double[] coordinates;

	/**
	 * Initializes a blank coordinate vector. Required for deserialization!
	 */
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
	 * @param point The dimension values of the point.
	 */
	public void setCoordinates(double[] coordinates) {
		this.coordinates = coordinates;
	}

	/**
	 * Computes the Euclidian distance between this coordinate vector and a
	 * second coordinate vector.
	 * 
	 * @param cv The coordinate vector to which the distance is computed.
	 * @return The Euclidian distance to coordinate vector cv. If cv has a
	 *         different length than this coordinate vector, -1 is returned.
	 */
	public double computeEuclidianDistance(CoordVector cv) {
		// check coordinate vector lengths
		if (cv.coordinates.length != this.coordinates.length) {
			return -1.0;
		}

		double quadSum = 0.0;
		for (int i = 0; i < this.coordinates.length; i++) {
			double diff = this.coordinates[i] - cv.coordinates[i];
			quadSum += diff*diff;
		}
		return Math.sqrt(quadSum);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {
		int length = in.readInt();
		this.coordinates = new double[length];
		for (int i = 0; i < length; i++) {
			this.coordinates[i] = in.readDouble();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.coordinates.length);
		for (int i = 0; i < this.coordinates.length; i++) {
			out.writeDouble(this.coordinates[i]);
		}
	}

	/**
	 * Compares this coordinate vector to another key.
	 * 
	 * @return -1 if the other key is not of type CoordVector. If the other
	 *         key is also a CoordVector but its length differs from this
	 *         coordinates vector, -1 is return if this coordinate vector is
	 *         smaller and 1 if it is larger. If both coordinate vectors
	 *         have the same length, the coordinates of both are compared.
	 *         If a coordinate of this coordinate vector is smaller than the
	 *         corresponding coordinate of the other vector -1 is returned
	 *         and 1 otherwise. If all coordinates are identical 0 is
	 *         returned.
	 */
	@Override
	public int compareTo(Key o) {
		// check if other key is also of type CoordVector
		if (!(o instanceof CoordVector)) {
			return -1;
		}
		// cast to CoordVector
		CoordVector oP = (CoordVector) o;

		// check if both coordinate vectors have identical lengths
		if (oP.coordinates.length > this.coordinates.length) {
			return -1;
		}
		else if (oP.coordinates.length < this.coordinates.length) {
			return 1;
		}

		// compare all coordinates
		for (int i = 0; i < this.coordinates.length; i++) {
			if (oP.coordinates[i] > this.coordinates[i]) {
				return -1;
			} else if (oP.coordinates[i] < this.coordinates[i]) {
				return 1;
			}
		}
		return 0;
	}
}