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

import java.io.Serializable;

/**
 * Summarizer is the base class to calculate summary and store , and Summary is the result of Summarizer.
 * It will compute sum, squareSum = sum(x_i*x_i), min, max, normL1.
 * All statistics can use summary to calculate from these statistics.
 */
public abstract class BaseSummarizer implements Serializable {

	/**
	 * sum of outerProduct of each row.
	 */
	DenseMatrix outerProduct;

	/**
	 * count.
	 */
	protected long count;

	/**
	 * if calculateOuterProduct is true, outerProduct will be calculate. default not calculate.
	 */
	boolean calculateOuterProduct = false;

	/**
	 * pearson correlation. https://en.wikipedia.org/wiki/Pearson_correlation_coefficient.
	 */
	public abstract CorrelationResult correlation();

	/**
	 * covariance: https://en.wikipedia.org/wiki/Covariance.
	 */
	public abstract DenseMatrix covariance();

	public DenseMatrix getOuterProduct() {
		return outerProduct;
	}

}
