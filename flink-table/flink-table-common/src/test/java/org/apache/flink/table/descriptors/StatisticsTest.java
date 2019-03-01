/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptors;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link Statistics}.
 */
public class StatisticsTest extends DescriptorTestBase {

	@Test(expected = ValidationException.class)
	public void testInvalidRowCount() {
		addPropertyAndVerify(descriptors().get(0), "statistics.row-count", "abx");
	}

	@Test(expected = ValidationException.class)
	public void testMissingName() {
		removePropertyAndVerify(descriptors().get(0), "statistics.columns.0.name");
	}

	// ----------------------------------------------------------------------------------------------

	@Override
	public List<Descriptor> descriptors() {
		Statistics desc1 = new Statistics()
			.rowCount(1000L)
			.columnStats("a", new ColumnStats(1L, 2L, 3.0, 4, 6, 5))
			.columnAvgLength("b", 42.0)
			.columnNullCount("a", 300L);

		Map<String, ColumnStats> map = new HashMap<String, ColumnStats>();
		map.put("a", new ColumnStats(null, 2L, 3.0, null, 6, 5));
		Statistics desc2 = new Statistics()
			.tableStats(new TableStats(32L, map));

		return Arrays.asList(desc1, desc2);
	}

	@Override
	public DescriptorValidator validator() {
		return new StatisticsValidator();
	}

	@Override
	public List<Map<String, String>> properties() {
		Map<String, String> props1 = new HashMap<String, String>() {
			{
				put("statistics.property-version", "1");
				put("statistics.row-count", "1000");
				put("statistics.columns.0.name", "a");
				put("statistics.columns.0.distinct-count", "1");
				put("statistics.columns.0.null-count", "300");
				put("statistics.columns.0.avg-length", "3.0");
				put("statistics.columns.0.max-length", "4");
				put("statistics.columns.0.max-value", "6");
				put("statistics.columns.0.min-value", "5");
				put("statistics.columns.1.name", "b");
				put("statistics.columns.1.avg-length", "42.0");
			}
		};

		Map<String, String> props2 = new HashMap<String, String>() {
			{
				put("statistics.property-version", "1");
				put("statistics.row-count", "32");
				put("statistics.columns.0.name", "a");
				put("statistics.columns.0.null-count", "2");
				put("statistics.columns.0.avg-length", "3.0");
				put("statistics.columns.0.max-value", "6");
				put("statistics.columns.0.min-value", "5");
			}
		};

		return Arrays.asList(props1, props2);
	}
}
