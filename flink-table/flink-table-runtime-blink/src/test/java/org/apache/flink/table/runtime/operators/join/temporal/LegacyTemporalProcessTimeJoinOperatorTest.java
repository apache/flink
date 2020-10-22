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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Harness tests for {@link LegacyTemporalProcessTimeJoinOperator}.
 */
public class LegacyTemporalProcessTimeJoinOperatorTest extends LegacyTemporalTimeJoinOperatorTestBase {

	private int keyIdx = 0;
	private InternalTypeInfo<RowData> rowType = InternalTypeInfo.ofFields(
		new BigIntType(),
		new VarCharType(VarCharType.MAX_LENGTH));
	private TypeInformation<RowData> keyType = InternalTypeInfo.ofFields();
	private BinaryRowDataKeySelector keySelector = new BinaryRowDataKeySelector(
		new int[]{keyIdx},
		rowType.toRowFieldTypes());
	private InternalTypeInfo<RowData> outputRowType = InternalTypeInfo.ofFields(
		new BigIntType(),
		new VarCharType(VarCharType.MAX_LENGTH),
		new BigIntType(),
		new VarCharType(VarCharType.MAX_LENGTH));
	private RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(outputRowType.toRowFieldTypes());

	/**
	 * Test proctime temporal join.
	 */
	@Test
	public void testProcTimeTemporalJoin() throws Exception {
		LegacyTemporalProcessTimeJoinOperator joinOperator = new LegacyTemporalProcessTimeJoinOperator(
			rowType,
			joinCondition,
			0,
			0);
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
			joinOperator);
		testHarness.open();
		testHarness.setProcessingTime(1);
		testHarness.processElement1(insertRecord(1L, "1a1"));

		testHarness.setProcessingTime(2);
		testHarness.processElement2(insertRecord(2L, "2a2"));

		testHarness.setProcessingTime(3);
		testHarness.processElement1(insertRecord(2L, "2a3"));

		testHarness.setProcessingTime(4);
		testHarness.processElement2(insertRecord(1L, "1a4"));

		testHarness.setProcessingTime(5);
		testHarness.processElement1(insertRecord(1L, "1a5"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(2L, "2a3", 2L, "2a2"));
		expectedOutput.add(insertRecord(1L, "1a5", 1L, "1a4"));
		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	/**
	 * Test proctime temporal join when set idle state retention.
	 */
	@Test
	public void testProcTimeTemporalJoinWithStateRetention() throws Exception {
		final int minRetentionTime = 10;
		final int maxRetentionTime = minRetentionTime * 3 / 2;
		LegacyTemporalProcessTimeJoinOperator joinOperator = new LegacyTemporalProcessTimeJoinOperator(
			rowType,
			joinCondition,
			minRetentionTime,
			maxRetentionTime);
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
			joinOperator);
		testHarness.open();
		testHarness.setProcessingTime(1);
		testHarness.processElement1(insertRecord(1L, "1a1"));

		testHarness.setProcessingTime(2);
		testHarness.processElement2(insertRecord(2L, "2a2"));

		testHarness.setProcessingTime(3);
		testHarness.processElement1(insertRecord(2L, "2a3"));

		testHarness.setProcessingTime(3 + maxRetentionTime);
		testHarness.processElement1(insertRecord(2L, "1a5"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(2L, "2a3", 2L, "2a2"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> createTestHarness(
		LegacyTemporalProcessTimeJoinOperator temporalJoinOperator) throws Exception {

		return new KeyedTwoInputStreamOperatorTestHarness<>(
			temporalJoinOperator,
			keySelector,
			keySelector,
			keyType);
	}
}
