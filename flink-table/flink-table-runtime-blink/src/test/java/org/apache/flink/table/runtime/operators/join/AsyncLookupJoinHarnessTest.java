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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedResultFutureWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Harness tests for {@link LookupJoinRunner} and {@link LookupJoinWithCalcRunner}.
 */
public class AsyncLookupJoinHarnessTest extends AsyncLookupJoinTestBase {

	private static final int ASYNC_BUFFER_CAPACITY = 100;
	private static final int ASYNC_TIMEOUT_MS = 3000;

	private final TypeSerializer<RowData> inSerializer = new RowDataSerializer(
		DataTypes.INT().getLogicalType(),
		DataTypes.STRING().getLogicalType());

	private final RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(new LogicalType[]{
		DataTypes.INT().getLogicalType(),
		DataTypes.STRING().getLogicalType(),
		DataTypes.INT().getLogicalType(),
		DataTypes.STRING().getLogicalType()
	});

	private InternalTypeInfo<RowData> rightRowTypeInfo = InternalTypeInfo.ofFields(
		DataTypes.INT().getLogicalType(),
		DataTypes.STRING().getLogicalType());
	private TypeInformation<?> fetcherReturnType = rightRowTypeInfo;

	@Test
	public void testTemporalInnerAsyncJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER);

		testHarness.open();

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(insertRecord(1, "a"));
			testHarness.processElement(insertRecord(2, "b"));
			testHarness.processElement(insertRecord(3, "c"));
			testHarness.processElement(insertRecord(4, "d"));
			testHarness.processElement(insertRecord(5, "e"));
		}

		// wait until all async collectors in the buffer have been emitted out.
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.endInput();
			testHarness.close();
		}

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testTemporalInnerAsyncJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITH_FILTER);

		testHarness.open();

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(insertRecord(1, "a"));
			testHarness.processElement(insertRecord(2, "b"));
			testHarness.processElement(insertRecord(3, "c"));
			testHarness.processElement(insertRecord(4, "d"));
			testHarness.processElement(insertRecord(5, "e"));
		}

		// wait until all async collectors in the buffer have been emitted out.
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.endInput();
			testHarness.close();
		}

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testTemporalLeftAsyncJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITHOUT_FILTER);

		testHarness.open();

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(insertRecord(1, "a"));
			testHarness.processElement(insertRecord(2, "b"));
			testHarness.processElement(insertRecord(3, "c"));
			testHarness.processElement(insertRecord(4, "d"));
			testHarness.processElement(insertRecord(5, "e"));
		}

		// wait until all async collectors in the buffer have been emitted out.
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.endInput();
			testHarness.close();
		}

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(2, "b", null, null));
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(5, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testTemporalLeftAsyncJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITH_FILTER);

		testHarness.open();

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(insertRecord(1, "a"));
			testHarness.processElement(insertRecord(2, "b"));
			testHarness.processElement(insertRecord(3, "c"));
			testHarness.processElement(insertRecord(4, "d"));
			testHarness.processElement(insertRecord(5, "e"));
		}

		// wait until all async collectors in the buffer have been emitted out.
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.endInput();
			testHarness.close();
		}

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(2, "b", null, null));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(5, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	// ---------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
			JoinType joinType,
			FilterOnTable filterOnTable) throws Exception {
		RichAsyncFunction<RowData, RowData> joinRunner;
		boolean isLeftJoin = joinType == JoinType.LEFT_JOIN;
		if (filterOnTable == FilterOnTable.WITHOUT_FILTER) {
			joinRunner = new AsyncLookupJoinRunner(
				new GeneratedFunctionWrapper(new TestingFetcherFunction()),
				new GeneratedResultFutureWrapper<>(new TestingFetcherResultFuture()),
				fetcherReturnType,
				rightRowTypeInfo,
				isLeftJoin,
				ASYNC_BUFFER_CAPACITY);
		} else {
			joinRunner = new AsyncLookupJoinWithCalcRunner(
				new GeneratedFunctionWrapper(new TestingFetcherFunction()),
				new GeneratedFunctionWrapper<>(new CalculateOnTemporalTable()),
				new GeneratedResultFutureWrapper<>(new TestingFetcherResultFuture()),
				fetcherReturnType,
				rightRowTypeInfo,
				isLeftJoin,
				ASYNC_BUFFER_CAPACITY);
		}

		return new OneInputStreamOperatorTestHarness<>(
			new AsyncWaitOperatorFactory<>(
				joinRunner,
				ASYNC_TIMEOUT_MS,
				ASYNC_BUFFER_CAPACITY,
				AsyncDataStream.OutputMode.ORDERED),
			inSerializer);
	}

	/**
	 * Whether this is a inner join or left join.
	 */
	private enum JoinType {
		INNER_JOIN,
		LEFT_JOIN
	}

	/**
	 * Whether there is a filter on temporal table.
	 */
	private enum FilterOnTable {
		WITH_FILTER,
		WITHOUT_FILTER
	}

	// ---------------------------------------------------------------------------------

	/**
	 * The {@link CalculateOnTemporalTable} is a filter on temporal table which only accepts
	 * length of name greater than or equal to 6.
	 */
	public static final class CalculateOnTemporalTable implements FlatMapFunction<RowData, RowData> {

		private static final long serialVersionUID = -1860345072157431136L;

		@Override
		public void flatMap(RowData value, Collector<RowData> out) throws Exception {
			BinaryStringData name = (BinaryStringData) value.getString(1);
			if (name.getSizeInBytes() >= 6) {
				out.collect(value);
			}
		}
	}
}
