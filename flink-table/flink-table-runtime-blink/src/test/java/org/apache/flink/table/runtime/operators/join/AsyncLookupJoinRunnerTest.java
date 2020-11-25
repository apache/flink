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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedResultFutureWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.junit.Test;

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

import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests for {@link AsyncLookupJoinRunner}.
 */
public class AsyncLookupJoinRunnerTest {

	@Test
	public void testCloseAsyncLookupJoinRunner() throws Exception {
		final InternalTypeInfo<RowData> rightRowTypeInfo = InternalTypeInfo.ofFields(
				DataTypes.INT().getLogicalType(),
				DataTypes.STRING().getLogicalType());
		final AsyncLookupJoinRunner joinRunner = new AsyncLookupJoinRunner(
				new GeneratedFunctionWrapper(new TestingFetcherFunction()),
				new GeneratedResultFutureWrapper<>(new TestingFetcherResultFuture()),
				rightRowTypeInfo,
				rightRowTypeInfo,
				true,
				100);
		assertNull(joinRunner.getAllResultFutures());
		closeAsyncLookupJoinRunner(joinRunner);

		joinRunner.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
		joinRunner.open(new Configuration());
		assertNotNull(joinRunner.getAllResultFutures());
		closeAsyncLookupJoinRunner(joinRunner);

		joinRunner.open(new Configuration());
		joinRunner.asyncInvoke(row(1, "a"), new TestingFetcherResultFuture());
		assertNotNull(joinRunner.getAllResultFutures());
		closeAsyncLookupJoinRunner(joinRunner);
	}

	private void closeAsyncLookupJoinRunner(AsyncLookupJoinRunner joinRunner) throws Exception {
		try {
			joinRunner.close();
		} catch (NullPointerException e) {
			fail("Expected close to fail with null pointer exception.");
		}
	}

	// ---------------------------------------------------------------------------------

	/**
	 * The {@link TestingFetcherFunction} only accepts a single integer lookup key and
	 * returns zero or one or more RowData.
	 */
	public static final class TestingFetcherFunction
			extends AbstractRichFunction
			implements AsyncFunction<RowData, RowData> {

		private static final long serialVersionUID = 4018474964018227081L;

		private static final Map<Integer, List<RowData>> data = new HashMap<>();

		static {
			data.put(1, Collections.singletonList(
					GenericRowData.of(1, fromString("Julian"))));
			data.put(3, Arrays.asList(
					GenericRowData.of(3, fromString("Jark")),
					GenericRowData.of(3, fromString("Jackson"))));
			data.put(4, Collections.singletonList(
					GenericRowData.of(4, fromString("Fabian"))));
		}

		private transient ExecutorService executor;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.executor = Executors.newSingleThreadExecutor();
		}

		@Override
		public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
			int id = input.getInt(0);
			CompletableFuture
					.supplyAsync((Supplier<Collection<RowData>>) () -> data.get(id), executor)
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
	public static final class TestingFetcherResultFuture extends TableFunctionResultFuture<RowData> {
		private static final long serialVersionUID = -312754413938303160L;

		@Override
		public void complete(Collection<RowData> result) {
			//noinspection unchecked
			getResultFuture().complete((Collection) result);
		}
	}
}
