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

import java.io.Serializable;

/**
 * The Vector class defines some common methods for both DenseVector and
 * SparseVector.
 */
public abstract class Vector implements Serializable {
	/**
	 * Get the size of the vector.
	 */
	public abstract int size();

	/**
	 * Get the i-th element of the vector.
	 */
	public abstract double get(int i);

	/**
	 * Set the i-th element of the vector to value "val".
	 */
	public abstract void set(int i, double val);

	/**
	 * Add the i-th element of the vector by value "val".
	 */
	public abstract void add(int i, double val);

	/**
	 * Return the L1 norm of the vector.
	 */
	public abstract double normL1();

	/**
	 * Return the Inf norm of the vector.
	 */
	public abstract double normInf();

	/**
	 * Return the L2 norm of the vector.
	 */
	public abstract double normL2();

	/**
	 * Return the square of L2 norm of the vector.
	 */
	public abstract double normL2Square();

	/**
	 * Scale the vector by value "v" and create a new vector to store the result.
	 */
	public abstract Vector scale(double v);

	/**
	 * Scale the vector by value "v".
	 */
	public abstract void scaleEqual(double v);

	/**
	 * Normalize the vector.
	 */
	public abstract void normalizeEqual(double p);

	/**
	 * Standardize the vector.
	 */
	public abstract void standardizeEqual(double mean, double stdvar);

	/**
	 * Create a new vector by adding an element to the head of the vector.
	 */
	public abstract Vector prefix(double v);

	/**
	 * Create a new vector by adding an element to the end of the vector.
	 */
	public abstract Vector append(double v);

	/**
	 * Create a new vector by plussing another vector.
	 */
	public abstract Vector plus(Vector vec);

	/**
	 * Create a new vector by subtracting  another vector.
	 */
	public abstract Vector minus(Vector vec);

	/**
	 * Compute the dot product with another vector.
	 */
	public abstract double dot(Vector vec);

	/**
	 * Get the iterator of the vector.
	 */
	public abstract VectorIterator iterator();

	/**
	 * Slice the vector.
	 */
	public abstract Vector slice(int[] indexes);

	/**
	 * Compute the outer product with itself.
	 *
	 * @return The outer product matrix.
	 */
	public abstract DenseMatrix outer();
}
