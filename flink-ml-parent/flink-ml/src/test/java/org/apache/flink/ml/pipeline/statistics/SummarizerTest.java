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

package org.apache.flink.ml.pipeline.statistics;

import org.apache.flink.ml.batchoperator.source.MemSourceBatchOp;
import org.apache.flink.ml.common.statistics.basicstatistic.TableSummary;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class SummarizerTest {
	@Test
	public void test() throws Exception {

		String[] colNames = new String[] {"id", "height", "weight"};

		Row[] data = new Row[] {
			Row.of(1, 168, 48.1),
			Row.of(2, 165, 45.8),
			Row.of(3, 160, 45.3),
			Row.of(4, 163, 41.9),
			Row.of(5, 149, 40.5),
		};

		Table input = new MemSourceBatchOp(data, colNames).getTable();

		TableSummary summary = new Summarizer(input).collectResult();

		System.out.println(summary.mean("height"));  // print the mean of the column(Name: “age”)

		System.out.println(summary);

		Assert.assertEquals(summary.mean("height"), 161.0, 1e-4);

	}
}
