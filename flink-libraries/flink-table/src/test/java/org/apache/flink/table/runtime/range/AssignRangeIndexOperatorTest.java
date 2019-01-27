/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.range;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.typeutils.TypeUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.table.dataformat.BinaryString.fromString;
import static org.apache.flink.table.runtime.join.batch.String2HashJoinOperatorTest.newRow;
import static org.mockito.Mockito.spy;

/**
 * UT for AssignRangeIndexOperator.
 */
public class AssignRangeIndexOperatorTest {

	@Test
	@SuppressWarnings("unchecked")
	public void testAssign() throws Exception {
		Object[][] ranges = new Object[3][];
		for (int i = 0; i < 3; i++) {
			ranges[i] = new Object[1];
		}
		ranges[0][0] = fromString("g");
		ranges[1][0] = fromString("m");
		ranges[2][0] = fromString("o");

		List<BinaryRow> data = new ArrayList<>();
		data.add(newRow("aaa", "0"));
		data.add(newRow("huj", "0"));
		data.add(newRow("o", "0"));
		data.add(newRow("xyz", "0"));

		BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(STRING_TYPE_INFO, STRING_TYPE_INFO);
		RowType type = (RowType) TypeConverters.createInternalTypeFromTypeInfo(typeInfo);
		int[] keys = new int[]{0};
		boolean[] orders = new boolean[]{true};
		scala.Tuple2<TypeComparator<?>[], TypeSerializer<?>[]> tuple2 =
				TypeUtils.flattenComparatorAndSerializer(
						typeInfo.getArity(), keys, orders, typeInfo.getFieldTypes());
		AssignRangeIndexOperator sampleAndHistogramOperator = new AssignRangeIndexOperator(
				new KeyExtractor(keys, orders, type.getFieldInternalTypes(), tuple2._1));

		TypeInformation<Object[][]> rangesTypeInfo = TypeExtractor.getForClass(Object[][].class);
		TupleTypeInfo<Tuple2<Integer, BinaryRow>> outTypeInfo = new TupleTypeInfo(BasicTypeInfo.INT_TYPE_INFO, typeInfo);
		TwoInputStreamTaskTestHarness<Object[][], BinaryRow, Tuple2<Integer, BinaryRow>> testHarness =
				new TwoInputStreamTaskTestHarness(env -> new TwoInputStreamTask((Environment) env), rangesTypeInfo, typeInfo, outTypeInfo);

		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getStreamConfig().setStreamOperator(sampleAndHistogramOperator);
		testHarness.getStreamConfig().setOperatorID(new OperatorID());

		StreamMockEnvironment environment = spy(testHarness.createEnvironment());
		testHarness.invoke(environment);
		testHarness.waitForTaskRunning();

		testHarness.processElement(new StreamRecord<>(ranges), 0, 0);

		for (BinaryRow row : data) {
			testHarness.processElement(new StreamRecord<>(row, 0), 1, 0);
		}

		testHarness.waitForInputProcessing();
		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		ConcurrentLinkedQueue<Object> expectedQueue = new ConcurrentLinkedQueue<>();
		expectedQueue.add(new StreamRecord<>(new Tuple2<>(0, newRow("aaa", "0"))));
		expectedQueue.add(new StreamRecord<>(new Tuple2<>(1, newRow("huj", "0"))));
		expectedQueue.add(new StreamRecord<>(new Tuple2<>(2, newRow("o", "0"))));
		expectedQueue.add(new StreamRecord<>(new Tuple2<>(3, newRow("xyz", "0"))));

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedQueue,
				testHarness.getOutput());
	}
}
