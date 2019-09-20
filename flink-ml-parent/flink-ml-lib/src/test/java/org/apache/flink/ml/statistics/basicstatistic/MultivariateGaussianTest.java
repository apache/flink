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

package org.apache.flink.ml.statistics.basicstatistic;

import org.apache.flink.ml.common.linalg.DenseMatrix;
import org.apache.flink.ml.common.linalg.DenseVector;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for MultivariateGaussian.
 */
public class MultivariateGaussianTest {
	private static final double TOL = 1.0e-5;

	@Test
	public void testMultivariate() throws Exception {
		DenseVector mu = DenseVector.zeros(2);

		DenseMatrix sigma1 = DenseMatrix.eye(2);
		MultivariateGaussian mg1 = new MultivariateGaussian(mu, sigma1);
		Assert.assertEquals(mg1.pdf(DenseVector.zeros(2)), 0.15915, TOL);
		Assert.assertEquals(mg1.pdf(DenseVector.ones(2)), 0.05855, TOL);

		DenseMatrix sigma2 = new DenseMatrix(2, 2, new double[]{4.0, -1.0, -1.0, 2.0});
		MultivariateGaussian mg2 = new MultivariateGaussian(mu, sigma2);
		Assert.assertEquals(mg2.pdf(DenseVector.zeros(2)), 0.060155, TOL);
		Assert.assertEquals(mg2.pdf(DenseVector.ones(2)), 0.033971, TOL);
	}

	@Test
	public void testMultivariateDegenerate() throws Exception {
		DenseVector mu = DenseVector.zeros(2);
		DenseMatrix sigma = new DenseMatrix(2, 2, new double[]{1.0, 1.0, 1.0, 1.0});
		MultivariateGaussian mg = new MultivariateGaussian(mu, sigma);
		Assert.assertEquals(mg.pdf(DenseVector.zeros(2)), 0.11254, TOL);
		Assert.assertEquals(mg.pdf(DenseVector.ones(2)), 0.068259, TOL);
	}
}
