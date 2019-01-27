/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.codegen.GeneratedSorter;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.apache.commons.lang3.RandomStringUtils;
import org.codehaus.commons.compiler.CompileException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.table.runtime.join.batch.String2HashJoinOperatorTest.newRow;
import static org.apache.flink.table.runtime.sort.InMemorySortTest.getSortBase;

/**
 * Test for {@link SortOperator}.
 */
public class SortOperatorTest {

	@Test
	public void testNormal() throws Exception {
		List<BinaryRow> data = new ArrayList<>();
		data.add(newRow("a", "0"));
		data.add(newRow("d", "0"));
		data.add(newRow("a", "2"));
		data.add(newRow("b", "1"));
		data.add(newRow("c", "2"));
		data.add(newRow("b", "4"));
		test(data);
	}

	@Test
	public void testRandom() throws Exception {
		List<BinaryRow> data = new ArrayList<>();
		Random random = new Random();
		for (int i = 0; i < 100; i++) {
			data.add(newRow(
					RandomStringUtils.random(random.nextInt(10)),
					RandomStringUtils.random(random.nextInt(10))));
		}
		test(data);
	}

	public void test(List<BinaryRow> rawData) throws Exception {
		TestSortOperator operator = new TestSortOperator();

		BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(STRING_TYPE_INFO, STRING_TYPE_INFO);
		OneInputStreamTaskTestHarness<BinaryRow, BinaryRow> testHarness =
				new OneInputStreamTaskTestHarness<>(OneInputStreamTask::new, 2, 2, (TypeInformation) typeInfo, typeInfo);

		testHarness.setupOutputForSingletonOperatorChain();
		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setStreamOperator(operator);
		streamConfig.setOperatorID(new OperatorID());

		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		for (BinaryRow row : rawData) {
			testHarness.processElement(new StreamRecord<>(row, initialTime));
		}

		testHarness.waitForInputProcessing();
		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		rawData.sort((o1, o2) -> {
			int cmp = o1.getBinaryString(0).compareTo(o2.getBinaryString(0));
			if (cmp == 0) {
				cmp = o1.getBinaryString(1).compareTo(o2.getBinaryString(1));
			}
			return cmp;
		});

		for (BinaryRow row : rawData) {
			expectedOutput.add(new StreamRecord<>(row));
		}
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				testHarness.getOutput());
	}

	/**
	 * Override cookGeneratedClasses.
	 */
	static class TestSortOperator extends SortOperator {

		static TypeSerializer[] serializers = new TypeSerializer[]{
				StringSerializer.INSTANCE, StringSerializer.INSTANCE
		};

		static TypeComparator[] comparators = new TypeComparator[]{
				new StringComparator(true),
				new StringComparator(true)
		};

		public TestSortOperator() {
			super(32 * 32 * 1024, 32 * 32 * 1024, 0, new GeneratedSorter(null, null, null, null));
		}

		@Override
		protected void cookGeneratedClasses(ClassLoader cl) throws CompileException {
			Tuple2<NormalizedKeyComputer, RecordComparator> base;
			TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING};
			try {
				base = getSortBase("SortOperatorTest", types, serializers, comparators,
						new int[]{0, 1}, new boolean[]{true, true});
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			computerClass = (Class<NormalizedKeyComputer>) base.f0.getClass();
			comparatorClass = (Class<RecordComparator>) base.f1.getClass();
		}
	}
}
