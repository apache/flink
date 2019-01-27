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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.codegen.CodeGeneratorContext;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.codegen.GeneratedSorter;
import org.apache.flink.table.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.codegen.SortCodeGenerator;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.plan.util.SortUtil;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.typeutils.TypeUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import scala.Tuple2;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.table.runtime.join.batch.String2HashJoinOperatorTest.newRow;
import static org.junit.Assert.assertEquals;

/**
 * UT for SampleAndHistogramOperator.
 */
public class SampleAndHistogramOperatorTest {

	@Test
	@SuppressWarnings("unchecked")
	public void test() throws Exception {
		List<IntermediateSampleData<BinaryRow>> data = new ArrayList<>();
		data.add(new IntermediateSampleData(0.01, newRow("aaa", "0")));
		data.add(new IntermediateSampleData(0.02, newRow("zzz", "0")));
		data.add(new IntermediateSampleData(0.99, newRow("ddd", "1")));
		data.add(new IntermediateSampleData(0.5, newRow("gfi", "2")));
		data.add(new IntermediateSampleData(0.2, newRow("a", "0")));
		data.add(new IntermediateSampleData(0.7, newRow("mkl", "3")));
		data.add(new IntermediateSampleData(0.85, newRow("sdf", "7")));
		data.add(new IntermediateSampleData(0.7, newRow("hkl", "4")));
		data.add(new IntermediateSampleData(0.2, newRow("a", "0")));
		data.add(new IntermediateSampleData(0.3, newRow("a", "0")));
		data.add(new IntermediateSampleData(0.9, newRow("jkl", "5")));
		data.add(new IntermediateSampleData(0.8, newRow("xyz", "6")));
		data.add(new IntermediateSampleData(0.78, newRow("njk", "10")));
		data.add(new IntermediateSampleData(0.86, newRow("oji", "9")));
		data.add(new IntermediateSampleData(0.752, newRow("efg", "8")));
		data.add(new IntermediateSampleData(0.3, newRow("a", "0")));

		TableConfig config = new TableConfig();

		//sort fields:
		// ddd, efg, gfi,hkl, jkl, mkl, njk, oji, sdf, xyz
		BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(STRING_TYPE_INFO, STRING_TYPE_INFO);
		RowType type = (RowType) TypeConverters.createInternalTypeFromTypeInfo(typeInfo);
		int[] keys = new int[]{0};
		boolean[] orders = new boolean[]{true};
		RowType dataType = (RowType) TypeConverters.createInternalTypeFromTypeInfo(typeInfo);
		GeneratedProjection copyToBinaryRow = ProjectionCodeGenerator.generateProjection(
				CodeGeneratorContext.apply(config, false), "copyToBinaryRow", dataType,
				dataType, new int[]{0, 1}, CodeGeneratorContext.DEFAULT_INPUT1_TERM(),
				CodeGeneratorContext.DEFAULT_OUT_RECORD_TERM(),
				CodeGeneratorContext.DEFAULT_OUT_RECORD_WRITER_TERM(), false, true);
		Tuple2<TypeComparator<?>[], TypeSerializer<?>[]> tuple2 =
				TypeUtils.flattenComparatorAndSerializer(
						typeInfo.getArity(), keys, orders, typeInfo.getFieldTypes());
		boolean[] nullsIsLast = SortUtil.getNullDefaultOrders(orders);
		SortCodeGenerator sortGen = new SortCodeGenerator(
				keys, dataType.getFieldInternalTypes(), tuple2._1, orders, nullsIsLast);
		SampleAndHistogramOperator sampleAndHistogramOperator = new SampleAndHistogramOperator(
			10, copyToBinaryRow,
				new GeneratedSorter(
						sortGen.generateNormalizedKeyComputer("SampleAndHistogramComputer"),
						sortGen.generateRecordComparator("SampleAndHistogramComparator"),
						tuple2._2, tuple2._1),
				new KeyExtractor(keys, orders, type.getFieldInternalTypes(), tuple2._1), 4);

		TypeInformation<IntermediateSampleData> inTypeInfo =
				TypeExtractor.getForClass(IntermediateSampleData.class);
		TypeInformation<Object[][]> outTypeInfo = TypeExtractor.getForClass(Object[][].class);
		OneInputStreamTaskTestHarness<IntermediateSampleData, Object[][]> testHarness =
				new OneInputStreamTaskTestHarness<>(OneInputStreamTask::new, 2, 2, inTypeInfo, outTypeInfo);

		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getStreamConfig().setStreamOperator(sampleAndHistogramOperator);
		testHarness.getStreamConfig().setOperatorID(new OperatorID());

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		for (IntermediateSampleData sampleData : data) {
			testHarness.processElement(new StreamRecord<>(sampleData, 0));
		}

		testHarness.waitForInputProcessing();
		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		LinkedBlockingQueue<Object> output = testHarness.getOutput();

		assertEquals(1, output.size());

		Object[][] range = (Object[][]) ((StreamRecord) output.iterator().next()).getValue();
		assertEquals(3, range.length);
		assertEquals("gfi", range[0][0].toString());
		assertEquals("mkl", range[1][0].toString());
		assertEquals("oji", range[2][0].toString());
	}
}
