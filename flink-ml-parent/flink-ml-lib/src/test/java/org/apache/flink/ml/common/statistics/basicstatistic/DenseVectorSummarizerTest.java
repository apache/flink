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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for DenseVectorSummarizer.
 */
public class DenseVectorSummarizerTest {

	@Test
	public void visit() {
		DenseVectorSummarizer summarizer = summarizer();

		assertEquals(5, summarizer.count);
		assertArrayEquals(new double[]{15.0, -15.0, 15.0}, summarizer.sum.getData(), 10e-6);
		assertArrayEquals(new double[]{55.0, 55.0, 45.0}, summarizer.squareSum.getData(), 10e-6);
		assertArrayEquals(new double[]{1.0, -5.0, 3.0}, summarizer.min.getData(), 10e-6);
		assertArrayEquals(new double[]{5.0, -1.0, 3.0}, summarizer.max.getData(), 10e-6);
		assertArrayEquals(new double[]{15.0, 15.0, 15.0}, summarizer.normL1.getData(), 10e-6);
	}

	@Test
	public void visitSparseVector() {
		DenseVectorSummarizer summarizer = summarizer();

		BaseVectorSummarizer addSparseSummarizer = summarizer.visit(new SparseVector(5, new int[]{3}, new double[]{1.0}));
		BaseVectorSummary addSparseSummary = addSparseSummarizer.toSummary();

		assertTrue(addSparseSummarizer instanceof SparseVectorSummarizer);

		assertEquals(6, addSparseSummarizer.count);

		assertArrayEquals(new double[]{15.0, -15.0, 15.0, 1.0, 0.0}, ((SparseVector) addSparseSummary.sum()).toDenseVector().getData(), 10e-6);
		assertArrayEquals(new double[]{7.416198, 7.416198, 6.708204, 1.0, 0.0}, ((SparseVector) addSparseSummary.normL2()).toDenseVector().getData(), 10e-6);
		assertArrayEquals(new double[]{1.0, -5.0, 3.0, 1.0, 0.0}, ((SparseVector) addSparseSummary.min()).toDenseVector().getData(), 10e-6);
		assertArrayEquals(new double[]{5.0, -1.0, 3.0, 1.0, 0.0}, ((SparseVector) addSparseSummary.max()).toDenseVector().getData(), 10e-6);
		assertArrayEquals(new double[]{15.0, 15.0, 15.0, 1.0, 0.0}, ((SparseVector) addSparseSummary.normL1()).toDenseVector().getData(), 10e-6);
	}

	@Test
	public void visitWithCov() {
		DenseVectorSummarizer summarizer = summarizerWithCov();

		assertEquals(5, summarizer.count);
		assertArrayEquals(new double[]{15.0, -15.0, 15.0}, summarizer.sum.getData(), 10e-6);
		assertArrayEquals(new double[]{55.0, 55.0, 45.0}, summarizer.squareSum.getData(), 10e-6);
		assertArrayEquals(new double[]{1.0, -5.0, 3.0}, summarizer.min.getData(), 10e-6);
		assertArrayEquals(new double[]{5.0, -1.0, 3.0}, summarizer.max.getData(), 10e-6);
		assertArrayEquals(new double[]{15.0, 15.0, 15.0}, summarizer.normL1.getData(), 10e-6);
		assertArrayEquals(new double[]{2.5, -2.5, 0.0, -2.5, 2.5, 0.0, 0.0, 0.0, 0.0},
			summarizer.covariance().getArrayCopy1D(true), 10e-6);
		assertArrayEquals(new double[]{1.0, -0.9999999999999999, 0.0, -0.9999999999999999, 1.0, 0.0, 0.0, 0.0, 1.0},
			summarizer.correlation().correlation.getArrayCopy1D(true), 10e-6);
	}

	@Test
	public void merge() {
		DenseVectorSummarizer summarizer = summarizer();
		DenseVectorSummarizer mergeSummarizer = (DenseVectorSummarizer) VectorSummarizerUtil.merge(summarizer, summarizer);

		assertEquals(10, summarizer.count);
		assertArrayEquals(new double[]{30.0, -30.0, 30.0}, mergeSummarizer.sum.getData(), 10e-6);
		assertArrayEquals(new double[]{110.0, 110.0, 90.0}, mergeSummarizer.squareSum.getData(), 10e-6);
		assertArrayEquals(new double[]{1.0, -5.0, 3.0}, mergeSummarizer.min.getData(), 10e-6);
		assertArrayEquals(new double[]{5.0, -1.0, 3.0}, mergeSummarizer.max.getData(), 10e-6);
		assertArrayEquals(new double[]{30.0, 30.0, 30.0}, mergeSummarizer.normL1.getData(), 10e-6);
	}

	@Test
	public void mergeWithCov() {
		DenseVectorSummarizer summarizer = summarizerWithCov();
		DenseVectorSummarizer mergeSummarizer = (DenseVectorSummarizer) VectorSummarizerUtil.merge(summarizer, summarizer);

		assertEquals(10, summarizer.count);
		assertArrayEquals(new double[]{30.0, -30.0, 30.0}, mergeSummarizer.sum.getData(), 10e-6);
		assertArrayEquals(new double[]{110.0, 110.0, 90.0}, mergeSummarizer.squareSum.getData(), 10e-6);
		assertArrayEquals(new double[]{1.0, -5.0, 3.0}, mergeSummarizer.min.getData(), 10e-6);
		assertArrayEquals(new double[]{5.0, -1.0, 3.0}, mergeSummarizer.max.getData(), 10e-6);
		assertArrayEquals(new double[]{30.0, 30.0, 30.0}, mergeSummarizer.normL1.getData(), 10e-6);
		assertArrayEquals(new double[]{2.2222222222222223, -2.2222222222222223, 0.0, -2.2222222222222223, 2.2222222222222223, 0.0, 0.0, 0.0, 0.0},
			summarizer.covariance().getArrayCopy1D(true), 10e-6);
		assertArrayEquals(new double[]{1.0, -0.9999999999999999, 0.0, -0.9999999999999999, 1.0, 0.0, 0.0, 0.0, 1.0},
			summarizer.correlation().correlation.getArrayCopy1D(true), 10e-6);
	}

	@Test
	public void mergeSparseSummarizer() {
		DenseVectorSummarizer denseVectorSummarizer = DenseVectorSummarizerTest.summarizer();
		SparseVectorSummarizer sparseVectorSummarizer = SparseVectorSummarizerTest.summarizer();

		SparseVectorSummarizer summarizer = (SparseVectorSummarizer)
			VectorSummarizerUtil.merge(denseVectorSummarizer, sparseVectorSummarizer);

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

	@Test
	public void toSummary() {
		DenseVectorSummary summary = (DenseVectorSummary) summarizer().toSummary();

		assertEquals(5, summary.count);
		assertArrayEquals(new double[]{15.0, -15.0, 15.0}, summary.sum.getData(), 10e-6);
		assertArrayEquals(new double[]{55.0, 55.0, 45.0}, summary.squareSum.getData(), 10e-6);
		assertArrayEquals(new double[]{1.0, -5.0, 3.0}, summary.min.getData(), 10e-6);
		assertArrayEquals(new double[]{5.0, -1.0, 3.0}, summary.max.getData(), 10e-6);
		assertArrayEquals(new double[]{15.0, 15.0, 15.0}, summary.normL1.getData(), 10e-6);
	}

	static DenseVectorSummarizer summarizer() {
		DenseVector[] data = geneData();
		DenseVectorSummarizer summarizer = new DenseVectorSummarizer();
		for (DenseVector aData : data) {
			summarizer.visit(aData);
		}
		return summarizer;
	}

	private static DenseVectorSummarizer summarizerWithCov() {
		DenseVector[] data = geneData();
		DenseVectorSummarizer summarizer = new DenseVectorSummarizer(true);
		for (DenseVector aData : data) {
			summarizer.visit(aData);
		}
		return summarizer;
	}

	private static DenseVector[] geneData() {
		return
			new DenseVector[]{
				new DenseVector(new double[]{1.0, -1.0, 3.0}),
				new DenseVector(new double[]{2.0, -2.0, 3.0}),
				new DenseVector(new double[]{3.0, -3.0, 3.0}),
				new DenseVector(new double[]{4.0, -4.0, 3.0}),
				new DenseVector(new double[]{5.0, -5.0, 3.0})
			};
	}
}
