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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.table.runtime.join.batch.String2HashJoinOperatorTest.newRow;

/**
 * UT for RemoveRangeIndexOperator.
 */
public class RemoveRangeIndexOperatorTest {

	@Test
	@SuppressWarnings("unchecked")
	public void removeRangeIndex() throws Exception {
		List<Tuple2<Integer, BinaryRow>> data = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			data.add(new Tuple2<>(i, newRow(String.valueOf(i), String.valueOf(i))));
		}
		RemoveRangeIndexOperator operator = new RemoveRangeIndexOperator();

		BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(STRING_TYPE_INFO, STRING_TYPE_INFO);
		TupleTypeInfo<Tuple2<Integer, BinaryRow>> inTypeInfo =
				new TupleTypeInfo(BasicTypeInfo.INT_TYPE_INFO, typeInfo);
		OneInputStreamTaskTestHarness<Tuple2<Integer, BinaryRow>, BinaryRow> testHarness =
				new OneInputStreamTaskTestHarness<>(
						OneInputStreamTask::new, 2, 2, (TypeInformation) inTypeInfo, typeInfo);

		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getStreamConfig().setStreamOperator(operator);
		testHarness.getStreamConfig().setOperatorID(new OperatorID());

		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		for (Tuple2<Integer, BinaryRow> tuple2 : data) {
			testHarness.processElement(new StreamRecord<>(tuple2, initialTime));
		}

		testHarness.waitForInputProcessing();
		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		LinkedBlockingQueue<Object> expectedOut = testHarness.getOutput();
		for (int i = 0; i < 100; i++) {
			expectedOut.add(new StreamRecord<>(newRow(String.valueOf(i), String.valueOf(i))));
		}
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOut,
				testHarness.getOutput());
	}
}
