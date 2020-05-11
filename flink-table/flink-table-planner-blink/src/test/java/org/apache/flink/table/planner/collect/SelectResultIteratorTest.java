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

package org.apache.flink.table.planner.collect;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.table.planner.collect.utils.TestingCoordinationRequestHandler;
import org.apache.flink.table.planner.collect.utils.TestingJobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Tests for {@link SelectResultIterator}.
 */
public class SelectResultIteratorTest extends TestLogger {

	private static final OperatorID TEST_OPERATOR_ID = new OperatorID();
	private static final JobID TEST_JOB_ID = new JobID();
	private static final String ACCUMULATOR_NAME = "accumulatorName";
	private static final TypeSerializer<Row> SERIALIZER = getSerializer();

	@Test
	public void testCheckpointed() throws Exception {
		// run this random test multiple times
		for (int testCount = 100; testCount > 0; testCount--) {
			List<Integer> expected = new ArrayList<>();
			for (int i = 0; i < 200; i++) {
				expected.add(i);
			}

			SelectResultIterator iterator = new SelectResultIterator(
				CompletableFuture.completedFuture(TEST_OPERATOR_ID),
				SERIALIZER,
				ACCUMULATOR_NAME,
				0);
			TestingJobClient jobClient = new TestingJobClient(
				TEST_JOB_ID,
				TEST_OPERATOR_ID,
				new TestingCoordinationRequestHandler(expected, SERIALIZER, ACCUMULATOR_NAME));
			iterator.setJobClient(jobClient);

			List<Row> actual = new ArrayList<>();
			while (iterator.hasNext()) {
				actual.add(iterator.next());
			}
			Assert.assertEquals(expected.size(), actual.size());

			List<Integer> actualExtracted = new ArrayList<>();
			for (Row row : actual) {
				Assert.assertEquals(1, row.getArity());
				actualExtracted.add((Integer) row.getField(0));
			}

			Collections.sort(expected);
			Collections.sort(actualExtracted);
			Assert.assertArrayEquals(expected.toArray(new Integer[0]), actualExtracted.toArray(new Integer[0]));

			iterator.close();
		}
	}

	private static TypeSerializer<Row> getSerializer() {
		return new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO).createSerializer(new ExecutionConfig());
	}
}
