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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("serial")
public class DistinctTranslationTest {

	@Test
	public void testCombinable() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<String> input = env.fromElements("1", "2", "1", "3");
			
			
			DistinctOperator<String> op = input.distinct(new KeySelector<String, String>() {
				public String getKey(String value) { return value; }
			});
			
			op.output(new DiscardingOutputFormat<String>());
			
			Plan p = env.createProgramPlan();
			
			GroupReduceOperatorBase<?, ?, ?> reduceOp = (GroupReduceOperatorBase<?, ?, ?>) p.getDataSinks().iterator().next().getInput();
			assertTrue(reduceOp.isCombinable());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void translateDistinctPlain() {
		try {
			final int parallelism = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);

			initialData.distinct().output(new DiscardingOutputFormat<Tuple3<Double, StringValue, LongValue>>());

			Plan p = env.createProgramPlan();

			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();

			// currently distinct is translated to a GroupReduce
			GroupReduceOperatorBase<?, ?, ?> reducer = (GroupReduceOperatorBase<?, ?, ?>) sink.getInput();

			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());

			// check keys
			assertArrayEquals(new int[] {0, 1, 2}, reducer.getKeyColumns(0));

			// parallelism was not configured on the operator
			assertTrue(reducer.getParallelism() == 1 || reducer.getParallelism() == -1);

			assertTrue(reducer.getInput() instanceof GenericDataSourceBase<?, ?>);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test caused an error: " + e.getMessage());
		}
	}

	@Test
	public void translateDistinctPlain2() {
		try {
			final int parallelism = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

			DataSet<CustomType> initialData = getSourcePojoDataSet(env);

			initialData.distinct().output(new DiscardingOutputFormat<CustomType>());

			Plan p = env.createProgramPlan();

			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();

			// currently distinct is translated to a GroupReduce
			GroupReduceOperatorBase<?, ?, ?> reducer = (GroupReduceOperatorBase<?, ?, ?>) sink.getInput();

			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());

			// check keys
			assertArrayEquals(new int[] {0}, reducer.getKeyColumns(0));

			// parallelism was not configured on the operator
			assertTrue(reducer.getParallelism() == 1 || reducer.getParallelism() == -1);

			assertTrue(reducer.getInput() instanceof GenericDataSourceBase<?, ?>);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test caused an error: " + e.getMessage());
		}
	}

	@Test
	public void translateDistinctPosition() {
		try {
			final int parallelism = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);

			initialData.distinct(1, 2).output(new DiscardingOutputFormat<Tuple3<Double, StringValue, LongValue>>());

			Plan p = env.createProgramPlan();

			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();

			// currently distinct is translated to a GroupReduce
			GroupReduceOperatorBase<?, ?, ?> reducer = (GroupReduceOperatorBase<?, ?, ?>) sink.getInput();

			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());

			// check keys
			assertArrayEquals(new int[] {1, 2}, reducer.getKeyColumns(0));

			// parallelism was not configured on the operator
			assertTrue(reducer.getParallelism() == 1 || reducer.getParallelism() == -1);

			assertTrue(reducer.getInput() instanceof GenericDataSourceBase<?, ?>);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test caused an error: " + e.getMessage());
		}
	}

	@Test
	public void translateDistinctKeySelector() {
		try {
			final int parallelism = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);

			initialData.distinct(new KeySelector<Tuple3<Double,StringValue,LongValue>, StringValue>() {
				public StringValue getKey(Tuple3<Double, StringValue, LongValue> value) {
					return value.f1;
				}
			}).setParallelism(4).output(new DiscardingOutputFormat<Tuple3<Double, StringValue, LongValue>>());

			Plan p = env.createProgramPlan();

			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();

			PlanUnwrappingReduceGroupOperator<?, ?, ?> reducer = (PlanUnwrappingReduceGroupOperator<?, ?, ?>) sink.getInput();
			MapOperatorBase<?, ?, ?> keyExtractor = (MapOperatorBase<?, ?, ?>) reducer.getInput();

			// check the parallelisms
			assertEquals(1, keyExtractor.getParallelism());
			assertEquals(4, reducer.getParallelism());

			// check types
			TypeInformation<?> keyValueInfo = new TupleTypeInfo<Tuple2<StringValue, Tuple3<Double,StringValue,LongValue>>>(
					new ValueTypeInfo<StringValue>(StringValue.class),
					initialData.getType());

			assertEquals(initialData.getType(), keyExtractor.getOperatorInfo().getInputType());
			assertEquals(keyValueInfo, keyExtractor.getOperatorInfo().getOutputType());

			assertEquals(keyValueInfo, reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());

			// check keys
			assertEquals(KeyExtractingMapper.class, keyExtractor.getUserCodeWrapper().getUserCodeClass());

			assertTrue(keyExtractor.getInput() instanceof GenericDataSourceBase<?, ?>);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test caused an error: " + e.getMessage());
		}
	}

	@Test
	public void translateDistinctExpressionKey() {
		try {
			final int parallelism = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

			DataSet<CustomType> initialData = getSourcePojoDataSet(env);

			initialData.distinct("myInt").output(new DiscardingOutputFormat<CustomType>());

			Plan p = env.createProgramPlan();

			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();

			// currently distinct is translated to a GroupReduce
			GroupReduceOperatorBase<?, ?, ?> reducer = (GroupReduceOperatorBase<?, ?, ?>) sink.getInput();

			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());

			// check keys
			assertArrayEquals(new int[] {0}, reducer.getKeyColumns(0));

			// parallelism was not configured on the operator
			assertTrue(reducer.getParallelism() == 1 || reducer.getParallelism() == -1);

			assertTrue(reducer.getInput() instanceof GenericDataSourceBase<?, ?>);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test caused an error: " + e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private static final DataSet<Tuple3<Double, StringValue, LongValue>> getSourceDataSet(ExecutionEnvironment env) {
		return env.fromElements(new Tuple3<Double, StringValue, LongValue>(3.141592, new StringValue("foobar"), new LongValue(77)))
				.setParallelism(1);
	}

	private static DataSet<CustomType> getSourcePojoDataSet(ExecutionEnvironment env) {
		List<CustomType> data = new ArrayList<CustomType>();
		data.add(new CustomType(1));
		return env.fromCollection(data);
	}

	public static class CustomType implements Serializable {

		private static final long serialVersionUID = 1L;
		public int myInt;

		public CustomType() {};

		public CustomType(int i) {
			myInt = i;
		}

		@Override
		public String toString() {
			return ""+myInt;
		}
	}
}
