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

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for DenseVectorSummary.
 */
public class DenseVectorSummaryTest {

	@Test
	public void test() {
		DenseVectorSummary srt = summary();

		Assert.assertEquals(3, srt.vectorSize());
		Assert.assertEquals(5, srt.count());
		Assert.assertEquals(-1.0, srt.max(1), 10e-4);
		Assert.assertEquals(-5.0, srt.min(1), 10e-4);
		Assert.assertEquals(-15.0, srt.sum(1), 10e-4);
		Assert.assertEquals(-3.0, srt.mean(1), 10e-4);
		Assert.assertEquals(2.5, srt.variance(1), 10e-4);
		Assert.assertEquals(1.5811, srt.standardDeviation(1), 10e-4);
		Assert.assertEquals(15.0, srt.normL1(1), 10e-4);
		Assert.assertEquals(7.416198, srt.normL2(1), 10e-4);

		Assert.assertArrayEquals(new double[]{5.0, -1.0, 3.0}, ((DenseVector) srt.max()).getData(), 10e-4);
		Assert.assertArrayEquals(new double[]{1.0, -5.0, 3.0}, ((DenseVector) srt.min()).getData(), 10e-4);
		Assert.assertArrayEquals(new double[]{15.0, -15.0, 15.0}, ((DenseVector) srt.sum()).getData(), 10e-4);
		Assert.assertArrayEquals(new double[]{3.0, -3.0, 3.0}, ((DenseVector) srt.mean()).getData(), 10e-4);
		Assert.assertArrayEquals(new double[]{2.5, 2.5, 0.0}, ((DenseVector) srt.variance()).getData(), 10e-4);
		Assert.assertArrayEquals(new double[]{1.5811, 1.5811, 0.0}, ((DenseVector) srt.standardDeviation()).getData(), 10e-4);
		Assert.assertArrayEquals(new double[]{15, 15, 15}, ((DenseVector) srt.normL1()).getData(), 10e-4);
		Assert.assertArrayEquals(new double[]{7.416198, 7.416198, 6.7082}, ((DenseVector) srt.normL2()).getData(), 10e-4);
	}

	private DenseVectorSummary summary() {
		DenseVector[] data =
			new DenseVector[]{
				new DenseVector(new double[]{1.0, -1.0, 3.0}),
				new DenseVector(new double[]{2.0, -2.0, 3.0}),
				new DenseVector(new double[]{3.0, -3.0, 3.0}),
				new DenseVector(new double[]{4.0, -4.0, 3.0}),
				new DenseVector(new double[]{5.0, -5.0, 3.0})
			};

		DenseVectorSummarizer summarizer = new DenseVectorSummarizer();
		for (DenseVector aData : data) {
			summarizer.visit(aData);
		}
		return (DenseVectorSummary) summarizer.toSummary();
	}

}
