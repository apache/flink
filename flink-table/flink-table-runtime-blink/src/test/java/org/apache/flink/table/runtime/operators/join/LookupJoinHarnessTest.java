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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollectorWrapper;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Harness tests for {@link LookupJoinRunner} and {@link LookupJoinWithCalcRunner}.
 */
public class LookupJoinHarnessTest {

	private final TypeSerializer<RowData> inSerializer = new RowDataSerializer(
		DataTypes.INT().getLogicalType(),
		DataTypes.STRING().getLogicalType());

	private final RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(new LogicalType[]{
		DataTypes.INT().getLogicalType(),
		DataTypes.STRING().getLogicalType(),
		DataTypes.INT().getLogicalType(),
		DataTypes.STRING().getLogicalType()
	});

	@Test
	public void testTemporalInnerJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalInnerJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITH_FILTER);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalLeftJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITHOUT_FILTER);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(2, "b", null, null));
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(5, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalLeftJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITH_FILTER);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(2, "b", null, null));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(5, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	// ---------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
			JoinType joinType,
			FilterOnTable filterOnTable) throws Exception {
		boolean isLeftJoin = joinType == JoinType.LEFT_JOIN;
		ProcessFunction<RowData, RowData> joinRunner;
		if (filterOnTable == FilterOnTable.WITHOUT_FILTER) {
			joinRunner = new LookupJoinRunner(
				new GeneratedFunctionWrapper<>(new TestingFetcherFunction()),
				new GeneratedCollectorWrapper<>(new TestingFetcherCollector()),
				isLeftJoin,
				2);
		} else {
			joinRunner = new LookupJoinWithCalcRunner(
				new GeneratedFunctionWrapper<>(new TestingFetcherFunction()),
				new GeneratedFunctionWrapper<>(new CalculateOnTemporalTable()),
				new GeneratedCollectorWrapper<>(new TestingFetcherCollector()),
				isLeftJoin,
				2);
		}

		ProcessOperator<RowData, RowData> operator = new ProcessOperator<>(joinRunner);
		return new OneInputStreamOperatorTestHarness<>(
			operator,
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
	 * The {@link TestingFetcherFunction} only accepts a single integer lookup key and
	 * returns zero or one or more RowData.
	 */
	public static final class TestingFetcherFunction implements FlatMapFunction<RowData, RowData> {

		private static final long serialVersionUID = 4018474964018227081L;

		private static final Map<Integer, List<GenericRowData>> data = new HashMap<>();

		static {
			data.put(1, Collections.singletonList(
				GenericRowData.of(1, fromString("Julian"))));
			data.put(3, Arrays.asList(
				GenericRowData.of(3, fromString("Jark")),
				GenericRowData.of(3, fromString("Jackson"))));
			data.put(4, Collections.singletonList(
				GenericRowData.of(4, fromString("Fabian"))));
		}

		@Override
		public void flatMap(RowData value, Collector<RowData> out) throws Exception {
			int id = value.getInt(0);
			List<GenericRowData> rows = data.get(id);
			if (rows != null) {
				for (GenericRowData row : rows) {
					out.collect(row);
				}
			}
		}
	}

	/**
	 * The {@link TestingFetcherCollector} is a simple implementation of
	 * {@link TableFunctionCollector} which combines left and right into a JoinedRowData.
	 */
	public static final class TestingFetcherCollector extends TableFunctionCollector {
		private static final long serialVersionUID = -312754413938303160L;

		@Override
		public void collect(Object record) {
			RowData left = (RowData) getInput();
			RowData right = (RowData) record;
			outputResult(new JoinedRowData(left, right));
		}
	}

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
