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

import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for TableSummarizer.
 */
public class TableSummarizerTest {

	@Test
	public void testVisitNotCov() {
		TableSummarizer summarizer = testVisit(false);

		Assert.assertEquals(4, summarizer.count);
		Assert.assertArrayEquals(new double[] {3.0, 3.0, 1.0}, summarizer.sum.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {5.0, 5.0, 17.0}, summarizer.sum2.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {0.0, 0.0, -3.0}, summarizer.min.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {2.0, 2.0, 2.0}, summarizer.max.getData(), 10e-4);

	}

	@Test
	public void testVisitWithCov() {
		TableSummarizer summarizer = testVisit(true);

		Assert.assertArrayEquals(new double[] {5.0, 5.0, -4.0,
			5.0, 5.0, -4.0,
			-4.0, -4.0, 17.0}, summarizer.dotProduction.getArrayCopy1D(true), 10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5, Double.NaN,
				Double.NaN, -2.5, -2.5, 8.333333333333334, Double.NaN,
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN},
			summarizer.covariance().getArrayCopy1D(true),
			10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -0.866, Double.NaN,
				Double.NaN, 1.0, 1.0, -0.866, Double.NaN,
				Double.NaN, -0.866, -0.866, 1.0, Double.NaN,
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN},
			summarizer.correlation().corr.getArrayCopy1D(true),
			10e-4);

	}

	@Test
	public void testMerge() {
		TableSummarizer summarizer = testWithMerge(false);

		Assert.assertEquals(4, summarizer.count);
		Assert.assertArrayEquals(new double[] {3.0, 3.0, 1.0}, summarizer.sum.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {5.0, 5.0, 17.0}, summarizer.sum2.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {0.0, 0.0, -3.0}, summarizer.min.getData(), 10e-4);
		Assert.assertArrayEquals(new double[] {2.0, 2.0, 2.0}, summarizer.max.getData(), 10e-4);

	}

	@Test
	public void testMergeWithCov() {
		TableSummarizer summarizer = testWithMerge(true);

		Assert.assertArrayEquals(new double[] {5.0, 5.0, -4.0,
			5.0, 5.0, -4.0,
			-4.0, -4.0, 17.0}, summarizer.dotProduction.getArrayCopy1D(true), 10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5, Double.NaN,
				Double.NaN, -2.5, -2.5, 8.333333333333334, Double.NaN,
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN},
			summarizer.covariance().getArrayCopy1D(true),
			10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -0.866, Double.NaN,
				Double.NaN, 1.0, 1.0, -0.866, Double.NaN,
				Double.NaN, -0.866, -0.866, 1.0, Double.NaN,
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN},
			summarizer.correlation().corr.getArrayCopy1D(true),
			10e-4);

	}

	@Test
	public void testClone() {
		TableSummarizer summarizer = testVisit(true).clone();

		Assert.assertArrayEquals(new double[] {5.0, 5.0, -4.0,
			5.0, 5.0, -4.0,
			-4.0, -4.0, 17.0}, summarizer.dotProduction.getArrayCopy1D(true), 10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5, Double.NaN,
				Double.NaN, 1.0, 1.0, -2.5, Double.NaN,
				Double.NaN, -2.5, -2.5, 8.333333333333334, Double.NaN,
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN},
			summarizer.covariance().getArrayCopy1D(true),
			10e-4);

		Assert.assertArrayEquals(new double[] {
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
				Double.NaN, 1.0, 1.0, -0.866, Double.NaN,
				Double.NaN, 1.0, 1.0, -0.866, Double.NaN,
				Double.NaN, -0.866, -0.866, 1.0, Double.NaN,
				Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN},
			summarizer.correlation().corr.getArrayCopy1D(true),
			10e-4);

	}

	private Row[] geneData() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"a", 1L, 1, 2.0, true}),
				Row.of(new Object[] {null, 2L, 2, -3.0, true}),
				Row.of(new Object[] {"c", null, null, 2.0, false}),
				Row.of(new Object[] {"a", 0L, 0, null, null}),
			};
		return testArray;
	}

	private TableSummarizer testVisit(boolean bCov) {
		Row[] data = geneData();
		int[] numberIdxs = new int[] {1, 2, 3};
		TableSummarizer summarizer = new TableSummarizer(bCov, numberIdxs);
		for (int i = 0; i < data.length; i++) {
			summarizer.visit(data[i]);
		}

		summarizer.colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};
		return summarizer;
	}

	private TableSummarizer testWithMerge(boolean bCov) {
		Row[] data = geneData();
		int[] numberIdxs = new int[] {1, 2, 3};

		TableSummarizer summarizerLeft = new TableSummarizer(bCov, numberIdxs);
		for (int i = 0; i < 2; i++) {
			summarizerLeft.visit(data[i]);
		}

		summarizerLeft.colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};

		TableSummarizer summarizerRight = new TableSummarizer(bCov, numberIdxs);
		for (int i = 2; i < 4; i++) {
			summarizerRight.visit(data[i]);
		}

		summarizerRight.colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};

		return summarizerLeft.merge(summarizerRight);
	}

}
