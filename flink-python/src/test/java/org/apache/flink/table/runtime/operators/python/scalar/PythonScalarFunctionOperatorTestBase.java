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

package org.apache.flink.table.runtime.operators.python.scalar;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.runtime.runners.python.scalar.AbstractPythonScalarFunctionRunnerTest;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Base class for Python scalar function operator test. These test that:
 *
 * <ul>
 *     <li>Retraction flag is correctly forwarded to the downstream</li>
 *     <li>FinishBundle is called when checkpoint is encountered</li>
 *     <li>Watermarks are buffered and only sent to downstream when finishedBundle is triggered</li>
 * </ul>
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the output elements.
 * @param <UDFIN> Type of the UDF input type.
 */
public abstract class PythonScalarFunctionOperatorTestBase<IN, OUT, UDFIN> {

	@Test
	public void testRetractionFieldKept() throws Exception {
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness(new Configuration());
		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
		testHarness.processElement(new StreamRecord<>(newRow(false, "c3", "c4", 1L), initialTime + 2));
		testHarness.processElement(new StreamRecord<>(newRow(false, "c5", "c6", 2L), initialTime + 3));
		testHarness.close();

		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(false, "c3", "c4", 1L)));
		expectedOutput.add(new StreamRecord<>(newRow(false, "c5", "c6", 2L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testFinishBundleTriggeredOnCheckpoint() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness(conf);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));

		// checkpoint trigger finishBundle
		testHarness.prepareSnapshotPreBarrier(0L);

		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testFinishBundleTriggeredByCount() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 2);
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness(conf);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
		assertOutputEquals("FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 1L), initialTime + 2));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 1L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testFinishBundleTriggeredByTime() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
		conf.setLong(PythonOptions.MAX_BUNDLE_TIME_MILLS, 1000L);
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness(conf);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
		assertOutputEquals("FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(1000L);
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));
		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testFinishBundleTriggeredByClose() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness(conf);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
		assertOutputEquals("FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

		testHarness.close();
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));
		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testWatermarkProcessedOnFinishBundle() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness(conf);
		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
		testHarness.processWatermark(initialTime + 2);
		assertOutputEquals("Watermark has been processed", expectedOutput, testHarness.getOutput());

		// checkpoint trigger finishBundle
		testHarness.prepareSnapshotPreBarrier(0L);

		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));
		expectedOutput.add(new Watermark(initialTime + 2));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testPythonScalarFunctionOperatorIsChainedByDefault() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tEnv = createTableEnvironment(env);
		tEnv.registerFunction("pyFunc", new PythonScalarFunction("pyFunc"));
		DataStream<Tuple2<Integer, Integer>> ds = env.fromElements(new Tuple2<>(1, 2));
		Table t = tEnv.fromDataStream(ds, $("a"), $("b")).select(call("pyFunc", $("a"), $("b")));
		// force generating the physical plan for the given table
		tEnv.toAppendStream(t, BasicTypeInfo.INT_TYPE_INFO);
		JobGraph jobGraph = env.getStreamGraph().getJobGraph();
		List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
		Assert.assertEquals(1, vertices.size());
	}

	private OneInputStreamOperatorTestHarness<IN, OUT> getTestHarness(Configuration config) throws Exception {
		RowType dataType = new RowType(Arrays.asList(
			new RowType.RowField("f1", new VarCharType()),
			new RowType.RowField("f2", new VarCharType()),
			new RowType.RowField("f3", new BigIntType())));
		AbstractPythonScalarFunctionOperator<IN, OUT, UDFIN> operator = getTestOperator(
			config,
			new PythonFunctionInfo[] {
				new PythonFunctionInfo(
					AbstractPythonScalarFunctionRunnerTest.DummyPythonFunction.INSTANCE,
					new Integer[]{0})
			},
			dataType,
			dataType,
			new int[]{2},
			new int[]{0, 1}
		);

		OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator);
		testHarness.getStreamConfig().setManagedMemoryFraction(0.5);
		testHarness.setup(getOutputTypeSerializer(dataType));
		return testHarness;
	}

	public abstract AbstractPythonScalarFunctionOperator<IN, OUT, UDFIN> getTestOperator(
		Configuration config,
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields);

	public abstract IN newRow(boolean accumulateMsg, Object... fields);

	public abstract void assertOutputEquals(String message, Collection<Object> expected, Collection<Object> actual);

	public abstract StreamTableEnvironment createTableEnvironment(StreamExecutionEnvironment env);

	public abstract TypeSerializer<OUT> getOutputTypeSerializer(RowType dataType);
}
