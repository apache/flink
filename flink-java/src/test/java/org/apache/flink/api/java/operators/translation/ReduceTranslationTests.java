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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for translation of reduce operation.
 */
@SuppressWarnings("serial")
public class ReduceTranslationTests implements java.io.Serializable {

	@Test
	public void translateNonGroupedReduce() {
		try {
			final int parallelism = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);

			initialData.reduce(new RichReduceFunction<Tuple3<Double, StringValue, LongValue>>() {
				public Tuple3<Double, StringValue, LongValue> reduce(Tuple3<Double, StringValue, LongValue> value1, Tuple3<Double, StringValue, LongValue> value2) {
					return value1;
				}
			}).output(new DiscardingOutputFormat<Tuple3<Double, StringValue, LongValue>>());

			Plan p = env.createProgramPlan();

			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();

			ReduceOperatorBase<?, ?> reducer = (ReduceOperatorBase<?, ?>) sink.getInput();

			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());

			// check keys
			assertTrue(reducer.getKeyColumns(0) == null || reducer.getKeyColumns(0).length == 0);

			// parallelism was not configured on the operator
			assertTrue(reducer.getParallelism() == 1 || reducer.getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT);

			assertTrue(reducer.getInput() instanceof GenericDataSourceBase<?, ?>);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test caused an error: " + e.getMessage());
		}
	}

	@Test
	public void translateGroupedReduceNoMapper() {
		try {
			final int parallelism = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);

			initialData
				.groupBy(2)
				.reduce(new RichReduceFunction<Tuple3<Double, StringValue, LongValue>>() {
					public Tuple3<Double, StringValue, LongValue> reduce(Tuple3<Double, StringValue, LongValue> value1, Tuple3<Double, StringValue, LongValue> value2) {
						return value1;
					}
				})
				.output(new DiscardingOutputFormat<Tuple3<Double, StringValue, LongValue>>());

			Plan p = env.createProgramPlan();

			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();

			ReduceOperatorBase<?, ?> reducer = (ReduceOperatorBase<?, ?>) sink.getInput();

			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());

			// parallelism was not configured on the operator
			assertTrue(reducer.getParallelism() == parallelism || reducer.getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT);

			// check keys
			assertArrayEquals(new int[] {2}, reducer.getKeyColumns(0));

			assertTrue(reducer.getInput() instanceof GenericDataSourceBase<?, ?>);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test caused an error: " + e.getMessage());
		}
	}

	@Test
	public void translateGroupedReduceWithkeyExtractor() {
		try {
			final int parallelism = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);

			initialData
				.groupBy(new KeySelector<Tuple3<Double, StringValue, LongValue>, StringValue>() {
					public StringValue getKey(Tuple3<Double, StringValue, LongValue> value) {
						return value.f1;
					}
				})
				.reduce(new RichReduceFunction<Tuple3<Double, StringValue, LongValue>>() {
					public Tuple3<Double, StringValue, LongValue> reduce(Tuple3<Double, StringValue, LongValue> value1, Tuple3<Double, StringValue, LongValue> value2) {
						return value1;
					}
				}).setParallelism(4)
				.output(new DiscardingOutputFormat<Tuple3<Double, StringValue, LongValue>>());

			Plan p = env.createProgramPlan();

			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();

			MapOperatorBase<?, ?, ?> keyProjector = (MapOperatorBase<?, ?, ?>) sink.getInput();
			PlanUnwrappingReduceOperator<?, ?> reducer = (PlanUnwrappingReduceOperator<?, ?>) keyProjector.getInput();
			MapOperatorBase<?, ?, ?> keyExtractor = (MapOperatorBase<?, ?, ?>) reducer.getInput();

			// check the parallelisms
			assertEquals(1, keyExtractor.getParallelism());
			assertEquals(4, reducer.getParallelism());
			assertEquals(4, keyProjector.getParallelism());

			// check types
			TypeInformation<?> keyValueInfo = new TupleTypeInfo<Tuple2<StringValue, Tuple3<Double, StringValue, LongValue>>>(
					new ValueTypeInfo<StringValue>(StringValue.class),
					initialData.getType());

			assertEquals(initialData.getType(), keyExtractor.getOperatorInfo().getInputType());
			assertEquals(keyValueInfo, keyExtractor.getOperatorInfo().getOutputType());

			assertEquals(keyValueInfo, reducer.getOperatorInfo().getInputType());
			assertEquals(keyValueInfo, reducer.getOperatorInfo().getOutputType());

			assertEquals(keyValueInfo, keyProjector.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), keyProjector.getOperatorInfo().getOutputType());

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

	@SuppressWarnings("unchecked")
	private static DataSet<Tuple3<Double, StringValue, LongValue>> getSourceDataSet(ExecutionEnvironment env) {
		return env.fromElements(new Tuple3<Double, StringValue, LongValue>(3.141592, new StringValue("foobar"), new LongValue(77)))
				.setParallelism(1);
	}
}
