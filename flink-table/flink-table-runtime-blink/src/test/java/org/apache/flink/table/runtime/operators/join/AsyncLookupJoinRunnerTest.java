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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedResultFutureWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.junit.Test;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests for {@link AsyncLookupJoinRunner}.
 */
public class AsyncLookupJoinRunnerTest extends AsyncLookupJoinTestBase {

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
			fail("Unexpected close to fail with null pointer exception.");
		}
	}
}
