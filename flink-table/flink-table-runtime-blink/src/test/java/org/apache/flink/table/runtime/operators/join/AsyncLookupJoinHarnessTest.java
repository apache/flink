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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedResultFutureWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static org.apache.flink.table.dataformat.BinaryString.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;

/**
 * Harness tests for {@link LookupJoinRunner} and {@link LookupJoinWithCalcRunner}.
 */
public class AsyncLookupJoinHarnessTest {

	private static final int ASYNC_BUFFER_CAPACITY = 100;
	private static final int ASYNC_TIMEOUT_MS = 3000;

	private final TypeSerializer<BaseRow> inSerializer = new BaseRowSerializer(
		new ExecutionConfig(),
		new IntType(),
		new VarCharType(VarCharType.MAX_LENGTH));

	private final BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(new TypeInformation[]{
		Types.INT,
		Types.STRING,
		Types.INT,
		Types.STRING
	});

	private BaseRowTypeInfo rightRowTypeInfo = new BaseRowTypeInfo(new IntType(), new VarCharType(VarCharType.MAX_LENGTH));
	private TypeInformation<?> fetcherReturnType = rightRowTypeInfo;

	@Test
	public void testTemporalInnerAsyncJoin() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER);

		testHarness.open();

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(record(1, "a"));
			testHarness.processElement(record(2, "b"));
			testHarness.processElement(record(3, "c"));
			testHarness.processElement(record(4, "d"));
			testHarness.processElement(record(5, "e"));
		}

		// wait until all async collectors in the buffer have been emitted out.
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.endInput();
			testHarness.close();
		}

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(3, "c", 3, "Jark"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testTemporalInnerAsyncJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITH_FILTER);

		testHarness.open();

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(record(1, "a"));
			testHarness.processElement(record(2, "b"));
			testHarness.processElement(record(3, "c"));
			testHarness.processElement(record(4, "d"));
			testHarness.processElement(record(5, "e"));
		}

		// wait until all async collectors in the buffer have been emitted out.
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.endInput();
			testHarness.close();
		}

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testTemporalLeftAsyncJoin() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITHOUT_FILTER);

		testHarness.open();

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(record(1, "a"));
			testHarness.processElement(record(2, "b"));
			testHarness.processElement(record(3, "c"));
			testHarness.processElement(record(4, "d"));
			testHarness.processElement(record(5, "e"));
		}

		// wait until all async collectors in the buffer have been emitted out.
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.endInput();
			testHarness.close();
		}

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(2, "b", null, null));
		expectedOutput.add(record(3, "c", 3, "Jark"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));
		expectedOutput.add(record(5, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testTemporalLeftAsyncJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITH_FILTER);

		testHarness.open();

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(record(1, "a"));
			testHarness.processElement(record(2, "b"));
			testHarness.processElement(record(3, "c"));
			testHarness.processElement(record(4, "d"));
			testHarness.processElement(record(5, "e"));
		}

		// wait until all async collectors in the buffer have been emitted out.
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.endInput();
			testHarness.close();
		}

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(2, "b", null, null));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));
		expectedOutput.add(record(5, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	// ---------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createHarness(
			JoinType joinType,
			FilterOnTable filterOnTable) throws Exception {
		RichAsyncFunction<BaseRow, BaseRow> joinRunner;
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
	 * The {@link TestingFetcherFunction} only accepts a single integer lookup key and
	 * returns zero or one or more BaseRows.
	 */
	public static final class TestingFetcherFunction
			extends AbstractRichFunction
			implements AsyncFunction<BaseRow, BaseRow> {

		private static final long serialVersionUID = 4018474964018227081L;

		private static final Map<Integer, List<BaseRow>> data = new HashMap<>();

		static {
			data.put(1, Collections.singletonList(
				GenericRow.of(1, fromString("Julian"))));
			data.put(3, Arrays.asList(
				GenericRow.of(3, fromString("Jark")),
				GenericRow.of(3, fromString("Jackson"))));
			data.put(4, Collections.singletonList(
				GenericRow.of(4, fromString("Fabian"))));
		}

		private transient ExecutorService executor;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.executor = Executors.newSingleThreadExecutor();
		}

		@Override
		public void asyncInvoke(BaseRow input, ResultFuture<BaseRow> resultFuture) throws Exception {
			int id = input.getInt(0);
			CompletableFuture
				.supplyAsync((Supplier<Collection<BaseRow>>) () -> data.get(id), executor)
				.thenAcceptAsync(resultFuture::complete, executor);
		}

		@Override
		public void close() throws Exception {
			super.close();
			if (null != executor && !executor.isShutdown()) {
				executor.shutdown();
			}
		}
	}

	/**
	 * The {@link TestingFetcherResultFuture} is a simple implementation of
	 * {@link TableFunctionCollector} which forwards the collected collection.
	 */
	public static final class TestingFetcherResultFuture extends TableFunctionResultFuture<BaseRow> {
		private static final long serialVersionUID = -312754413938303160L;

		@Override
		public void complete(Collection<BaseRow> result) {
			//noinspection unchecked
			getResultFuture().complete((Collection) result);
		}
	}

	/**
	 * The {@link CalculateOnTemporalTable} is a filter on temporal table which only accepts
	 * length of name greater than or equal to 6.
	 */
	public static final class CalculateOnTemporalTable implements FlatMapFunction<BaseRow, BaseRow> {

		private static final long serialVersionUID = -1860345072157431136L;

		@Override
		public void flatMap(BaseRow value, Collector<BaseRow> out) throws Exception {
			BinaryString name = value.getString(1);
			if (name.getSizeInBytes() >= 6) {
				out.collect(value);
			}
		}
	}
}
