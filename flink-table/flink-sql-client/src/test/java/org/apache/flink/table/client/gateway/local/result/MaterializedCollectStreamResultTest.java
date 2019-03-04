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

package org.apache.flink.table.client.gateway.local.result;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MaterializedCollectStreamResult}.
 */
public class MaterializedCollectStreamResultTest {

	@Test
	public void testSnapshot() throws UnknownHostException {
		final TypeInformation<Row> type = Types.ROW(Types.STRING, Types.LONG);

		TestMaterializedCollectStreamResult<?> result = null;
		try {
			result = new TestMaterializedCollectStreamResult<>(
				type,
				new ExecutionConfig(),
				InetAddress.getLocalHost(),
				0,
				Integer.MAX_VALUE);

			result.isRetrieving = true;

			result.processRecord(Tuple2.of(true, Row.of("A", 1)));
			result.processRecord(Tuple2.of(true, Row.of("B", 1)));
			result.processRecord(Tuple2.of(true, Row.of("A", 1)));
			result.processRecord(Tuple2.of(true, Row.of("C", 2)));

			assertEquals(TypedResult.payload(4), result.snapshot(1));

			assertEquals(Collections.singletonList(Row.of("A", 1)), result.retrievePage(1));
			assertEquals(Collections.singletonList(Row.of("B", 1)), result.retrievePage(2));
			assertEquals(Collections.singletonList(Row.of("A", 1)), result.retrievePage(3));
			assertEquals(Collections.singletonList(Row.of("C", 2)), result.retrievePage(4));

			result.processRecord(Tuple2.of(false, Row.of("A", 1)));

			assertEquals(TypedResult.payload(3), result.snapshot(1));

			assertEquals(Collections.singletonList(Row.of("A", 1)), result.retrievePage(1));
			assertEquals(Collections.singletonList(Row.of("B", 1)), result.retrievePage(2));
			assertEquals(Collections.singletonList(Row.of("C", 2)), result.retrievePage(3));

			result.processRecord(Tuple2.of(false, Row.of("C", 2)));
			result.processRecord(Tuple2.of(false, Row.of("A", 1)));

			assertEquals(TypedResult.payload(1), result.snapshot(1));

			assertEquals(Collections.singletonList(Row.of("B", 1)), result.retrievePage(1));
		} finally {
			if (result != null) {
				result.close();
			}
		}
	}

	@Test
	public void testLimitedSnapshot() throws UnknownHostException {
		final TypeInformation<Row> type = Types.ROW(Types.STRING, Types.LONG);

		TestMaterializedCollectStreamResult<?> result = null;
		try {
			result = new TestMaterializedCollectStreamResult<>(
				type,
				new ExecutionConfig(),
				InetAddress.getLocalHost(),
				0,
				2,  // limit the materialized table to 2 rows
				3); // with 3 rows overcommitment

			result.isRetrieving = true;

			result.processRecord(Tuple2.of(true, Row.of("D", 1)));
			result.processRecord(Tuple2.of(true, Row.of("A", 1)));
			result.processRecord(Tuple2.of(true, Row.of("B", 1)));
			result.processRecord(Tuple2.of(true, Row.of("A", 1)));

			assertEquals(
				Arrays.asList(null, null, Row.of("B", 1), Row.of("A", 1)), // two over-committed rows
				result.getMaterializedTable());

			assertEquals(TypedResult.payload(2), result.snapshot(1));

			assertEquals(Collections.singletonList(Row.of("B", 1)), result.retrievePage(1));
			assertEquals(Collections.singletonList(Row.of("A", 1)), result.retrievePage(2));

			result.processRecord(Tuple2.of(true, Row.of("C", 1)));

			assertEquals(
				Arrays.asList(Row.of("A", 1), Row.of("C", 1)), // limit clean up has taken place
				result.getMaterializedTable());

			result.processRecord(Tuple2.of(false, Row.of("A", 1)));

			assertEquals(
				Collections.singletonList(Row.of("C", 1)), // regular clean up has taken place
				result.getMaterializedTable());
		} finally {
			if (result != null) {
				result.close();
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	private static class TestMaterializedCollectStreamResult<T> extends MaterializedCollectStreamResult<T> {

		public boolean isRetrieving;

		public TestMaterializedCollectStreamResult(
				TypeInformation<Row> outputType,
				ExecutionConfig config,
				InetAddress gatewayAddress,
				int gatewayPort,
				int maxRowCount,
				int overcommitThreshold) {

			super(
				outputType,
				config,
				gatewayAddress,
				gatewayPort,
				maxRowCount,
				overcommitThreshold);
		}

		public TestMaterializedCollectStreamResult(
				TypeInformation<Row> outputType,
				ExecutionConfig config,
				InetAddress gatewayAddress,
				int gatewayPort,
				int maxRowCount) {

			super(
				outputType,
				config,
				gatewayAddress,
				gatewayPort,
				maxRowCount);
		}

		@Override
		protected boolean isRetrieving() {
			return isRetrieving;
		}

		@Override
		public List<Row> getMaterializedTable() {
			return super.getMaterializedTable();
		}
	}
}
