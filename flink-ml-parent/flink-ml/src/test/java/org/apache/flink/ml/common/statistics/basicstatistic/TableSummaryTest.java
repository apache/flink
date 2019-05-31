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
 * Test for TableSummary.
 */
public class TableSummaryTest {

	@Test
	public void test() {
		TableSummary srt = testVisit(false);

		Assert.assertEquals(srt.colNum(), 5);
		Assert.assertEquals(srt.count(), 4);
		Assert.assertEquals(srt.numMissingValue("f_double"), 1, 10e-4);
		Assert.assertEquals(srt.numValidValue("f_double"), 3, 10e-4);
		Assert.assertEquals(srt.max("f_double"), 2.0, 10e-4);
		Assert.assertEquals(srt.min("f_int"), 0.0, 10e-4);
		Assert.assertEquals(srt.mean("f_double"), 0.3333333333333333, 10e-4);
		Assert.assertEquals(srt.variance("f_double"), 8.333333333333334, 10e-4);
		Assert.assertEquals(srt.standardDeviation("f_double"), 2.886751345948129, 10e-4);
		Assert.assertEquals(srt.normL1("f_double"), 7.0, 10e-4);
		Assert.assertEquals(srt.normL2("f_double"), 4.123105625617661, 10e-4);

	}

	@Test
	public void testClone() {
		TableSummary srt = testVisit(false).clone();

		Assert.assertEquals(srt.colNum(), 5);
		Assert.assertEquals(srt.count(), 4);
		Assert.assertEquals(srt.numMissingValue("f_double"), 1, 10e-4);
		Assert.assertEquals(srt.numValidValue("f_double"), 3, 10e-4);
		Assert.assertEquals(srt.max("f_double"), 2.0, 10e-4);
		Assert.assertEquals(srt.min("f_int"), 0.0, 10e-4);
		Assert.assertEquals(srt.mean("f_double"), 0.3333333333333333, 10e-4);
		Assert.assertEquals(srt.variance("f_double"), 8.333333333333334, 10e-4);
		Assert.assertEquals(srt.standardDeviation("f_double"), 2.886751345948129, 10e-4);
		Assert.assertEquals(srt.normL1("f_double"), 7.0, 10e-4);
		Assert.assertEquals(srt.normL2("f_double"), 4.123105625617661, 10e-4);

	}

	private TableSummary testVisit(boolean bCov) {
		Row[] data = geneData();
		int[] numberIdxs = new int[] {1, 2, 3};
		TableSummarizer summarizer = new TableSummarizer(bCov, numberIdxs);
		for (int i = 0; i < data.length; i++) {
			summarizer.visit(data[i]);
		}

		summarizer.colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};
		return summarizer.toSummary();
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

}
