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

import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for SparseVectorSummarizer.
 */
public class SparseVectorSummarizerTest {

	@Test
	public void testEmpty() {
		SparseVectorSummarizer summarizer = new SparseVectorSummarizer();

		Assert.assertEquals(-1, summarizer.colNum);
		Assert.assertEquals(0, summarizer.cols.size());
	}

	@Test
	public void testVisit() {
		SparseVectorSummarizer summarizer = summarizer();

		Assert.assertEquals(5, summarizer.colNum);
		Assert.assertEquals(5, summarizer.count);
		Assert.assertEquals(5, summarizer.cols.size());
		Assert.assertEquals(10, summarizer.cols.get(0).sum, 10e-6);
		Assert.assertEquals(5, summarizer.cols.get(0).max, 10e-6);
		Assert.assertEquals(1, summarizer.cols.get(0).min, 10e-6);
		Assert.assertEquals(3, summarizer.cols.get(0).numNonZero, 10e-6);
		Assert.assertEquals(10, summarizer.cols.get(0).normL1, 10e-6);
		Assert.assertEquals(42, summarizer.cols.get(0).squareSum, 10e-6);
	}

	@Test
	public void testVisitDenseVector() {
		SparseVectorSummarizer summarizer = summarizer();

		SparseVectorSummarizer addDenseSummarizer =
			(SparseVectorSummarizer) summarizer.visit(new DenseVector(new double[]{-1.0, 2.0, 3.0}));

		Assert.assertEquals(5, addDenseSummarizer.colNum);
		Assert.assertEquals(6, addDenseSummarizer.count);
		Assert.assertEquals(5, addDenseSummarizer.cols.size());
		Assert.assertEquals(9, addDenseSummarizer.cols.get(0).sum, 10e-6);
		Assert.assertEquals(5, addDenseSummarizer.cols.get(0).max, 10e-6);
		Assert.assertEquals(-1, addDenseSummarizer.cols.get(0).min, 10e-6);
		Assert.assertEquals(4, addDenseSummarizer.cols.get(0).numNonZero, 10e-6);
		Assert.assertEquals(11, addDenseSummarizer.cols.get(0).normL1, 10e-6);
		Assert.assertEquals(43, addDenseSummarizer.cols.get(0).squareSum, 10e-6);
	}

	@Test
	public void testVisitWithCov() {
		SparseVectorSummarizer summarizer = summarizerWithCov();

		Assert.assertEquals(5, summarizer.colNum);
		Assert.assertEquals(5, summarizer.count);
		Assert.assertEquals(5, summarizer.cols.size());
		Assert.assertEquals(10, summarizer.cols.get(0).sum, 10e-6);
		Assert.assertEquals(5, summarizer.cols.get(0).max, 10e-6);
		Assert.assertEquals(1, summarizer.cols.get(0).min, 10e-6);
		Assert.assertEquals(3, summarizer.cols.get(0).numNonZero, 10e-6);
		Assert.assertEquals(10, summarizer.cols.get(0).normL1, 10e-6);
		Assert.assertEquals(42, summarizer.cols.get(0).squareSum, 10e-6);

		Assert.assertArrayEquals(new double[]{
				5.5, -4.5, -3.25, 1.5, 0.75,
				-4.5, 6.7, -1.75, 2.1, -2.55,
				-3.25, -1.75, 9.5, -6.75, 2.25,
				1.5, 2.1, -6.75, 6.3, -3.15,
				0.75, -2.55, 2.25, -3.15, 2.7},
			summarizer.covariance().getArrayCopy1D(true), 10e-6);

		Assert.assertArrayEquals(new double[]{
				1.0, -0.7412996032400851, -0.44961440151294857, 0.2548235957188128, 0.19462473604038075,
				-0.7412996032400851, 1.0, -0.2193506090927217, 0.3232299675777271, -0.5995437734798085,
				-0.44961440151294857, -0.2193506090927217, 1.0, -0.8725125761206665, 0.4442616583193193,
				0.2548235957188128, 0.3232299675777271, -0.8725125761206665, 1.0, -0.7637626158259733,
				0.19462473604038075, -0.5995437734798085, 0.4442616583193193, -0.7637626158259733, 1.0},
			summarizer.correlation().correlation.getArrayCopy1D(true), 10e-6);
	}

	@Test
	public void testMerge() {
		SparseVectorSummarizer summarizer = summarizer();
		summarizer = (SparseVectorSummarizer) VectorSummarizerUtil.merge(summarizer, summarizer);

		Assert.assertEquals(5, summarizer.colNum);
		Assert.assertEquals(10, summarizer.count);
		Assert.assertEquals(5, summarizer.cols.size());
		Assert.assertEquals(20, summarizer.cols.get(0).sum, 10e-6);
		Assert.assertEquals(5, summarizer.cols.get(0).max, 10e-6);
		Assert.assertEquals(1, summarizer.cols.get(0).min, 10e-6);
		Assert.assertEquals(6, summarizer.cols.get(0).numNonZero, 10e-6);
		Assert.assertEquals(20, summarizer.cols.get(0).normL1, 10e-6);
		Assert.assertEquals(84, summarizer.cols.get(0).squareSum, 10e-6);
	}

	@Test
	public void testMergeWithCov() {
		SparseVectorSummarizer summarizer = summarizerWithCov();
		summarizer = (SparseVectorSummarizer) VectorSummarizerUtil.merge(summarizer, summarizer);

		Assert.assertEquals(5, summarizer.colNum);
		Assert.assertEquals(10, summarizer.count);
		Assert.assertEquals(5, summarizer.cols.size());
		Assert.assertEquals(20, summarizer.cols.get(0).sum, 10e-6);
		Assert.assertEquals(5, summarizer.cols.get(0).max, 10e-6);
		Assert.assertEquals(1, summarizer.cols.get(0).min, 10e-6);
		Assert.assertEquals(6, summarizer.cols.get(0).numNonZero, 10e-6);
		Assert.assertEquals(20, summarizer.cols.get(0).normL1, 10e-6);
		Assert.assertEquals(84, summarizer.cols.get(0).squareSum, 10e-6);

		Assert.assertArrayEquals(new double[]{
				4.888888888888889, -4.0, -2.888888888888889, 1.3333333333333333, 0.6666666666666666,
				-4.0, 5.955555555555556, -1.5555555555555556, 1.8666666666666667, -2.2666666666666666, -2.888888888888889, -
				1.5555555555555556, 8.444444444444445, -6.0, 2.0, 1.3333333333333333,
				1.8666666666666667, -6.0, 5.6, -2.8, 0.6666666666666666,
				-2.2666666666666666, 2.0, -2.8, 2.4000000000000004},
			summarizer.covariance().getArrayCopy1D(true), 10e-6);

		Assert.assertArrayEquals(new double[]{
				1.0, -0.7412996032400851, -0.44961440151294857, 0.2548235957188128, 0.19462473604038075,
				-0.7412996032400851, 1.0, -0.2193506090927217, 0.3232299675777271, -0.5995437734798085,
				-0.44961440151294857, -0.2193506090927217, 1.0, -0.8725125761206665, 0.4442616583193193,
				0.2548235957188128, 0.3232299675777271, -0.8725125761206665, 1.0, -0.7637626158259733,
				0.19462473604038075, -0.5995437734798085, 0.4442616583193193, -0.7637626158259733, 1.0},
			summarizer.correlation().correlation.getArrayCopy1D(true), 10e-6);
	}

	@Test
	public void mergeDenseSummarizer() {
		DenseVectorSummarizer denseVectorSummarizer = DenseVectorSummarizerTest.summarizer();
		SparseVectorSummarizer sparseVectorSummarizer = SparseVectorSummarizerTest.summarizer();

		SparseVectorSummarizer summarizer = (SparseVectorSummarizer)
			VectorSummarizerUtil.merge(sparseVectorSummarizer, denseVectorSummarizer);

		Assert.assertEquals(5, summarizer.colNum);
		Assert.assertEquals(10, summarizer.count);
		Assert.assertEquals(5, summarizer.cols.size());
		Assert.assertEquals(25, summarizer.cols.get(0).sum, 10e-6);
		Assert.assertEquals(5, summarizer.cols.get(0).max, 10e-6);
		Assert.assertEquals(1, summarizer.cols.get(0).min, 10e-6);
		Assert.assertEquals(25, summarizer.cols.get(0).normL1, 10e-6);
		Assert.assertEquals(97, summarizer.cols.get(0).squareSum, 10e-6);
		Assert.assertEquals(8, summarizer.cols.get(0).numNonZero, 10e-6);
	}

	static SparseVectorSummarizer summarizer() {
		SparseVector[] data = geneData();
		SparseVectorSummarizer summarizer = new SparseVectorSummarizer();
		for (int i = 0; i < data.length; i++) {
			summarizer.visit(data[i]);
		}
		return summarizer;
	}

	private static SparseVectorSummarizer summarizerWithCov() {
		SparseVector[] data = geneData();
		SparseVectorSummarizer summarizer = new SparseVectorSummarizer(true);
		for (int i = 0; i < data.length; i++) {
			summarizer.visit(data[i]);
		}
		return summarizer;
	}

	private static SparseVector[] geneData() {
		SparseVector[] data =
			new SparseVector[]{
				new SparseVector(5, new int[]{0, 1, 2}, new double[]{1.0, -1.0, 3.0}),
				new SparseVector(5, new int[]{1, 2, 3}, new double[]{2.0, -2.0, 3.0}),
				new SparseVector(5, new int[]{2, 3, 4}, new double[]{3.0, -3.0, 3.0}),
				new SparseVector(5, new int[]{0, 2, 3}, new double[]{4.0, -4.0, 3.0}),
				new SparseVector(5, new int[]{0, 1, 4}, new double[]{5.0, -5.0, 3.0})
			};
		return data;
	}
}
