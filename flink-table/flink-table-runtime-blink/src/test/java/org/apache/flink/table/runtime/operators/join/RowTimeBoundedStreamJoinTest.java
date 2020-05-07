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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link RowTimeBoundedStreamJoin}.
 */
public class RowTimeBoundedStreamJoinTest extends TimeBoundedStreamJoinTestBase {

	private int keyIdx = 1;
	private BinaryRowDataKeySelector keySelector = new BinaryRowDataKeySelector(new int[] { keyIdx },
			rowType.getLogicalTypes());
	private TypeInformation<RowData> keyType = new RowDataTypeInfo();

	/** a.rowtime >= b.rowtime - 10 and a.rowtime <= b.rowtime + 20. **/
	@Test
	public void testRowTimeInnerJoinWithCommonBounds() throws Exception {
		RowTimeBoundedStreamJoin joinProcessFunc = new RowTimeBoundedStreamJoin(
				FlinkJoinType.INNER, -10, 20, 0, rowType, rowType, generatedFunction, 0, 0);

		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
				joinProcessFunc);

		testHarness.open();

		testHarness.processWatermark1(new Watermark(1));
		testHarness.processWatermark2(new Watermark(1));

		// Test late data.
		testHarness.processElement1(insertRecord(1L, "k1"));
		// Though (1L, "k1") is actually late, it will also be cached.
		assertEquals(1, testHarness.numEventTimeTimers());

		testHarness.processElement1(insertRecord(2L, "k1"));
		testHarness.processElement2(insertRecord(2L, "k1"));

		assertEquals(2, testHarness.numEventTimeTimers());
		assertEquals(4, testHarness.numKeyedStateEntries());

		testHarness.processElement1(insertRecord(5L, "k1"));
		testHarness.processElement2(insertRecord(15L, "k1"));
		testHarness.processWatermark1(new Watermark(20));
		testHarness.processWatermark2(new Watermark(20));
		assertEquals(4, testHarness.numKeyedStateEntries());

		testHarness.processElement1(insertRecord(35L, "k1"));

		// The right rows with timestamp = 2 and 5 will be removed here.
		// The left rows with timestamp = 2 and 15 will be removed here.
		testHarness.processWatermark1(new Watermark(38));
		testHarness.processWatermark2(new Watermark(38));

		testHarness.processElement1(insertRecord(40L, "k2"));
		testHarness.processElement2(insertRecord(39L, "k2"));
		assertEquals(6, testHarness.numKeyedStateEntries());

		// The right row with timestamp = 35 will be removed here.
		testHarness.processWatermark1(new Watermark(61));
		testHarness.processWatermark2(new Watermark(61));
		assertEquals(4, testHarness.numKeyedStateEntries());

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(new Watermark(-19));
		// This result is produced by the late row (1, "k1").
		expectedOutput.add(insertRecord(1L, "k1", 2L, "k1"));
		expectedOutput.add(insertRecord(2L, "k1", 2L, "k1"));
		expectedOutput.add(insertRecord(5L, "k1", 2L, "k1"));
		expectedOutput.add(insertRecord(5L, "k1", 15L, "k1"));
		expectedOutput.add(new Watermark(0));
		expectedOutput.add(insertRecord(35L, "k1", 15L, "k1"));
		expectedOutput.add(new Watermark(18));
		expectedOutput.add(insertRecord(40L, "k2", 39L, "k2"));
		expectedOutput.add(new Watermark(41));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	/** a.rowtime >= b.rowtime - 10 and a.rowtime <= b.rowtime - 7. **/
	@Test
	public void testRowTimeInnerJoinWithNegativeBounds() throws Exception {
		RowTimeBoundedStreamJoin joinProcessFunc = new RowTimeBoundedStreamJoin(
				FlinkJoinType.INNER, -10, -7, 0, rowType, rowType, generatedFunction, 0, 0);

		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
				joinProcessFunc);

		testHarness.open();

		testHarness.processWatermark1(new Watermark(1));
		testHarness.processWatermark2(new Watermark(1));

		// This row will not be cached.
		testHarness.processElement2(insertRecord(2L, "k1"));
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processWatermark1(new Watermark(2));
		testHarness.processWatermark2(new Watermark(2));
		testHarness.processElement1(insertRecord(3L, "k1"));
		testHarness.processElement2(insertRecord(3L, "k1"));

		// Test for -10 boundary (13 - 10 = 3).
		// This row from the right stream will be cached.
		// The clean time for the left stream is 13 - 7 + 1 - 1 = 8
		testHarness.processElement2(insertRecord(13L, "k1"));

		// Test for -7 boundary (13 - 7 = 6).
		testHarness.processElement1(insertRecord(6L, "k1"));
		assertEquals(4, testHarness.numKeyedStateEntries());

		// Trigger the left timer with timestamp  8.
		// The row with timestamp = 13 will be removed here (13 < 10 + 7).
		testHarness.processWatermark1(new Watermark(10));
		testHarness.processWatermark2(new Watermark(10));
		assertEquals(2, testHarness.numKeyedStateEntries());

		// Clear the states.
		testHarness.processWatermark1(new Watermark(18));
		testHarness.processWatermark2(new Watermark(18));
		assertEquals(0, testHarness.numKeyedStateEntries());

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(new Watermark(-9));
		expectedOutput.add(new Watermark(-8));
		expectedOutput.add(insertRecord(3L, "k1", 13L, "k1"));
		expectedOutput.add(insertRecord(6L, "k1", 13L, "k1"));
		expectedOutput.add(new Watermark(0));
		expectedOutput.add(new Watermark(8));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testRowTimeLeftOuterJoin() throws Exception {
		RowTimeBoundedStreamJoin joinProcessFunc = new RowTimeBoundedStreamJoin(
				FlinkJoinType.LEFT, -5, 9, 0, rowType, rowType, generatedFunction, 0, 0);

		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
				joinProcessFunc);

		testHarness.open();

		testHarness.processElement1(insertRecord(1L, "k1"));
		testHarness.processElement2(insertRecord(1L, "k2"));
		assertEquals(2, testHarness.numEventTimeTimers());
		assertEquals(4, testHarness.numKeyedStateEntries());

		// The left row with timestamp = 1 will be padded and removed (14=1+5+1+((5+9)/2)).
		testHarness.processWatermark1(new Watermark(14));
		testHarness.processWatermark2(new Watermark(14));
		assertEquals(1, testHarness.numEventTimeTimers());
		assertEquals(2, testHarness.numKeyedStateEntries());

		// The right row with timestamp = 1 will be removed (18=1+9+1+((5+9)/2)).
		testHarness.processWatermark1(new Watermark(18));
		testHarness.processWatermark2(new Watermark(18));
		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement1(insertRecord(2L, "k1"));
		testHarness.processElement2(insertRecord(2L, "k2"));
		// The late rows with timestamp = 2 will not be cached, but a null padding result for the left
		// row will be emitted.
		assertEquals(0, testHarness.numKeyedStateEntries());
		assertEquals(0, testHarness.numEventTimeTimers());

		// Make sure the common (inner) join can be performed.
		testHarness.processElement1(insertRecord(19L, "k1"));
		testHarness.processElement1(insertRecord(20L, "k1"));
		testHarness.processElement2(insertRecord(26L, "k1"));
		testHarness.processElement2(insertRecord(25L, "k1"));
		testHarness.processElement1(insertRecord(21L, "k1"));
		testHarness.processElement2(insertRecord(39L, "k2"));
		testHarness.processElement2(insertRecord(40L, "k2"));
		testHarness.processElement1(insertRecord(50L, "k2"));
		testHarness.processElement1(insertRecord(49L, "k2"));
		testHarness.processElement2(insertRecord(41L, "k2"));
		testHarness.processWatermark1(new Watermark(100));
		testHarness.processWatermark2(new Watermark(100));

		List<Object> expectedOutput = new ArrayList<>();
		// The timestamp 14 is set with the triggered timer.
		expectedOutput.add(insertRecord(1L, "k1", null, null));
		expectedOutput.add(new Watermark(5));
		expectedOutput.add(new Watermark(9));
		expectedOutput.add(insertRecord(2L, "k1", null, null));
		expectedOutput.add(insertRecord(20L, "k1", 25L, "k1"));
		expectedOutput.add(insertRecord(21L, "k1", 25L, "k1"));
		expectedOutput.add(insertRecord(21L, "k1", 26L, "k1"));
		expectedOutput.add(insertRecord(49L, "k2", 40L, "k2"));
		expectedOutput.add(insertRecord(49L, "k2", 41L, "k2"));
		expectedOutput.add(insertRecord(50L, "k2", 41L, "k2"));
		// The timestamp 32 is set with the triggered timer.
		expectedOutput.add(insertRecord(19L, "k1", null, null));
		expectedOutput.add(new Watermark(91));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testRowTimeRightOuterJoin() throws Exception {
		RowTimeBoundedStreamJoin joinProcessFunc = new RowTimeBoundedStreamJoin(
				FlinkJoinType.RIGHT, -5, 9, 0, rowType, rowType, generatedFunction, 0, 0);

		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
				joinProcessFunc);

		testHarness.open();

		testHarness.processElement1(insertRecord(1L, "k1"));
		testHarness.processElement2(insertRecord(1L, "k2"));
		assertEquals(2, testHarness.numEventTimeTimers());
		assertEquals(4, testHarness.numKeyedStateEntries());

		// The left row with timestamp = 1 will be removed (14=1+5+1+((5+9)/2)).
		testHarness.processWatermark1(new Watermark(14));
		testHarness.processWatermark2(new Watermark(14));
		assertEquals(1, testHarness.numEventTimeTimers());
		assertEquals(2, testHarness.numKeyedStateEntries());

		// The right row with timestamp = 1 will be padded and removed (18=1+9+1+((5+9)/2)).
		testHarness.processWatermark1(new Watermark(18));
		testHarness.processWatermark2(new Watermark(18));
		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement1(insertRecord(2L, "k1"));
		testHarness.processElement2(insertRecord(2L, "k2"));
		// The late rows with timestamp = 2 will not be cached, but a null padding result for the right
		// row will be emitted.
		assertEquals(0, testHarness.numKeyedStateEntries());
		assertEquals(0, testHarness.numEventTimeTimers());

		// Make sure the common (inner) join can be performed.
		testHarness.processElement1(insertRecord(19L, "k1"));
		testHarness.processElement1(insertRecord(20L, "k1"));
		testHarness.processElement2(insertRecord(26L, "k1"));
		testHarness.processElement2(insertRecord(25L, "k1"));
		testHarness.processElement1(insertRecord(21L, "k1"));
		testHarness.processElement2(insertRecord(39L, "k2"));
		testHarness.processElement2(insertRecord(40L, "k2"));
		testHarness.processElement1(insertRecord(50L, "k2"));
		testHarness.processElement1(insertRecord(49L, "k2"));
		testHarness.processElement2(insertRecord(41L, "k2"));
		testHarness.processWatermark1(new Watermark(100));
		testHarness.processWatermark2(new Watermark(100));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(new Watermark(5));
		// The timestamp 18 is set with the triggered timer.
		expectedOutput.add(insertRecord(null, null, 1L, "k2"));
		expectedOutput.add(new Watermark(9));
		expectedOutput.add(insertRecord(null, null, 2L, "k2"));
		expectedOutput.add(insertRecord(20L, "k1", 25L, "k1"));
		expectedOutput.add(insertRecord(21L, "k1", 25L, "k1"));
		expectedOutput.add(insertRecord(21L, "k1", 26L, "k1"));
		expectedOutput.add(insertRecord(49L, "k2", 40L, "k2"));
		expectedOutput.add(insertRecord(49L, "k2", 41L, "k2"));
		expectedOutput.add(insertRecord(50L, "k2", 41L, "k2"));
		// The timestamp 56 is set with the triggered timer.
		expectedOutput.add(insertRecord(null, null, 39L, "k2"));
		expectedOutput.add(new Watermark(91));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	/** a.rowtime >= b.rowtime - 5 and a.rowtime <= b.rowtime + 9. **/
	@Test
	public void testRowTimeFullOuterJoin() throws Exception {
		RowTimeBoundedStreamJoin joinProcessFunc = new RowTimeBoundedStreamJoin(
				FlinkJoinType.FULL, -5, 9, 0, rowType, rowType, generatedFunction, 0, 0);

		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
				joinProcessFunc);

		testHarness.open();

		testHarness.processElement1(insertRecord(1L, "k1"));
		testHarness.processElement2(insertRecord(1L, "k2"));
		assertEquals(2, testHarness.numEventTimeTimers());
		assertEquals(4, testHarness.numKeyedStateEntries());

		// The left row with timestamp = 1 will be padded and removed (14=1+5+1+((5+9)/2)).
		testHarness.processWatermark1(new Watermark(14));
		testHarness.processWatermark2(new Watermark(14));
		assertEquals(1, testHarness.numEventTimeTimers());
		assertEquals(2, testHarness.numKeyedStateEntries());

		// The right row with timestamp = 1 will be padded and removed (18=1+9+1+((5+9)/2)).
		testHarness.processWatermark1(new Watermark(18));
		testHarness.processWatermark2(new Watermark(18));
		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement1(insertRecord(2L, "k1"));
		testHarness.processElement2(insertRecord(2L, "k2"));
		// The late rows with timestamp = 2 will not be cached, but a null padding result for the right
		// row will be emitted.
		assertEquals(0, testHarness.numKeyedStateEntries());
		assertEquals(0, testHarness.numEventTimeTimers());

		// Make sure the common (inner) join can be performed.
		testHarness.processElement1(insertRecord(19L, "k1"));
		testHarness.processElement1(insertRecord(20L, "k1"));
		testHarness.processElement2(insertRecord(26L, "k1"));
		testHarness.processElement2(insertRecord(25L, "k1"));
		testHarness.processElement1(insertRecord(21L, "k1"));

		testHarness.processElement2(insertRecord(39L, "k2"));
		testHarness.processElement2(insertRecord(40L, "k2"));
		testHarness.processElement1(insertRecord(50L, "k2"));
		testHarness.processElement1(insertRecord(49L, "k2"));
		testHarness.processElement2(insertRecord(41L, "k2"));
		testHarness.processWatermark1(new Watermark(100));
		testHarness.processWatermark2(new Watermark(100));

		List<Object> expectedOutput = new ArrayList<>();
		// The timestamp 14 is set with the triggered timer.
		expectedOutput.add(insertRecord(1L, "k1", null, null));
		expectedOutput.add(new Watermark(5));
		// The timestamp 18 is set with the triggered timer.
		expectedOutput.add(insertRecord(null, null, 1L, "k2"));
		expectedOutput.add(new Watermark(9));
		expectedOutput.add(insertRecord(2L, "k1", null, null));
		expectedOutput.add(insertRecord(null, null, 2L, "k2"));
		expectedOutput.add(insertRecord(20L, "k1", 25L, "k1"));
		expectedOutput.add(insertRecord(21L, "k1", 25L, "k1"));
		expectedOutput.add(insertRecord(21L, "k1", 26L, "k1"));
		expectedOutput.add(insertRecord(49L, "k2", 40L, "k2"));
		expectedOutput.add(insertRecord(49L, "k2", 41L, "k2"));
		expectedOutput.add(insertRecord(50L, "k2", 41L, "k2"));
		// The timestamp 32 is set with the triggered timer.
		expectedOutput.add(insertRecord(19L, "k1", null, null));
		// The timestamp 56 is set with the triggered timer.
		expectedOutput.add(insertRecord(null, null, 39L, "k2"));
		expectedOutput.add(new Watermark(91));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> createTestHarness(
			RowTimeBoundedStreamJoin windowJoinFunc)
			throws Exception {
		KeyedCoProcessOperator<RowData, RowData, RowData, RowData> operator = new KeyedCoProcessOperatorWithWatermarkDelay<>(
				windowJoinFunc, windowJoinFunc.getMaxOutputDelay());
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
				new KeyedTwoInputStreamOperatorTestHarness<>(operator, keySelector, keySelector, keyType);
		return testHarness;
	}

}
