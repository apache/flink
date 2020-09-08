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

package org.apache.flink.connector.hbase;

import org.apache.flink.connector.hbase.source.HBaseRowInputFormat;
import org.apache.flink.connector.hbase.source.TableInputSplit;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase.util.HBaseTestBase;
import org.apache.flink.connector.hbase.util.PlannerType;

import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test case for {@link HBaseRowInputFormat} table input format.
 */
public class HBaseTableInputFormatTest extends HBaseTestBase {

	protected PlannerType planner() {
		return PlannerType.BLINK_PLANNER;
	}

	@Test
	public void testOpenClose() throws IOException {
		HBaseTableSchema tableSchema = new HBaseTableSchema();
		tableSchema.addColumn(FAMILY1, F1COL1, byte[].class);
		HBaseRowInputFormat inputFormat = new HBaseRowInputFormat(getConf(), TEST_TABLE_1, tableSchema);
		TableInputSplit[] tableInputSplits = inputFormat.createInputSplits(1);

		inputFormat.open(tableInputSplits[0]);
		assertNotNull(inputFormat.getConnection());
		assertNotNull(inputFormat.getConnection().getTable(TableName.valueOf(TEST_TABLE_1)));

		inputFormat.close();
		assertNull(inputFormat.getConnection());
	}
}
