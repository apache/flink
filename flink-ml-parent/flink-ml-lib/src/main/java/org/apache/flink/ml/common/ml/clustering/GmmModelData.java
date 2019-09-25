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

package org.apache.flink.ml.common.ml.clustering;

import org.apache.flink.ml.common.linalg.DenseMatrix;
import org.apache.flink.ml.common.linalg.DenseVector;

import java.io.Serializable;
import java.util.List;

/**
 * Model data of the Gaussian Mixture model.
 */
public class GmmModelData {
	/**
	 * Number of clusters.
	 */
	public int k;

	/**
	 * Feature dimension.
	 */
	public int dim;

	/**
	 * Name of the vector column of the training data.
	 */
	public String vectorCol;

	/**
	 * Cluster summaries of each clusters.
	 */
	public List<ClusterSummary> data;

	/**
	 * Summary of a Gaussian Mixture Model cluster.
	 */
	public static class ClusterSummary implements Serializable {
		/**
		 * Id of the cluster.
		 */
		public long clusterId;

		/**
		 * Weight of the cluster.
		 */
		public double weight;

		/**
		 * Mean of Gaussian distribution.
		 */
		public DenseVector mean;

		/**
		 * Compressed covariance matrix by storing the lower triangle part.
		 */
		public DenseVector cov;

		/**
		 * The default constructor.
		 */
		public ClusterSummary() {
		}

		/**
		 * The constructor.
		 *
		 * @param clusterId Id of the cluster.
		 * @param weight    Weight of the cluster.
		 * @param mean      Mean of Gaussian distribution.
		 * @param cov       Compressed covariance matrix by storing the lower triangle part.
		 */
		public ClusterSummary(long clusterId, double weight, DenseVector mean, DenseVector cov) {
			this.clusterId = clusterId;
			this.weight = weight;
			this.mean = mean;
			this.cov = cov;
		}
	}

	/**
	 * Expand the compressed covariance matrix to a full matrix.
	 *
	 * @param cov Compressed covariance matrix by storing the lower triangle part.
	 * @param n   Feature size.
	 * @return The expanded covariance matrix.
	 */
	public static DenseMatrix expandCovarianceMatrix(DenseVector cov, int n) {
		DenseMatrix mat = new DenseMatrix(n, n);
		int pos = 0;
		for (int i = 0; i < n; i++) {
			System.arraycopy(cov.getData(), pos, mat.getData(), i * n + i, n - i);
			pos += n - i;
		}
		assert pos == cov.size();
		for (int i = 0; i < n; i++) {
			for (int j = i + 1; j < n; j++) {
				mat.set(i, j, mat.get(j, i));
			}
		}
		return mat;
	}
}
