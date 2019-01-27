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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.dataformat.Decimal;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for HiveMetadataUtil.
 */
public class HiveMetadataUtilTest {
	@Test
	public void testDecimalConversion() {
		// Precision = length(longValue)
		Decimal flinkDecimal = Decimal.fromLong(111111, 6, 2);
		org.apache.hadoop.hive.metastore.api.Decimal hiveDecimal = HiveMetadataUtil.fromFlinkDecimal(flinkDecimal);

		assertEquals(flinkDecimal, HiveMetadataUtil.fromHiveDecimal(hiveDecimal));
		assertEquals(hiveDecimal, HiveMetadataUtil.fromFlinkDecimal(flinkDecimal));

		// Precision > length(longValue)
		flinkDecimal = Decimal.fromLong(111111, 8, 2);
		hiveDecimal = HiveMetadataUtil.fromFlinkDecimal(flinkDecimal);

		assertEquals(flinkDecimal, HiveMetadataUtil.fromHiveDecimal(hiveDecimal));
		assertEquals(hiveDecimal, HiveMetadataUtil.fromFlinkDecimal(flinkDecimal));
	}
}
