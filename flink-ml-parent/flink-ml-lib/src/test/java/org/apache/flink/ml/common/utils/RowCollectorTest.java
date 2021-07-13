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

package org.apache.flink.ml.common.utils;

import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for RowCollector.
 */
public class RowCollectorTest {

	@Test
	public void testRowCollector() {
		List<Row> rows = new ArrayList<>(1);

		RowCollector collector = new RowCollector(rows);

		collector.collect(Row.of(2));
		Assert.assertEquals("test size of RowCollector fail", 1, collector.getRows().size());
		Assert.assertEquals("test value of RowCollector fail", 2, collector.getRows().get(0).getField(0));

		collector.clear();
		Assert.assertEquals("test clear of RowCollector fail", 0, collector.getRows().size());

		collector.collect(Row.of(0));

		collector.close();
		Assert.assertEquals("test close of RowCollector fail", 0, collector.getRows().size());
	}
}
