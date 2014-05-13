/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.api.java.operators.translation;

import static org.junit.Assert.*;

import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.base.GenericDataSinkBase;
import eu.stratosphere.api.common.operators.base.GenericDataSourceBase;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.common.operators.base.ReduceOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.ValueTypeInfo;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.TypeInformation;

@SuppressWarnings("serial")
public class ReduceTranslationTests implements java.io.Serializable {

	@Test
	public void translateNonGroupedReduce() {
		try {
			final int DOP = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(DOP);
			
			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);
			
			initialData.reduce(new ReduceFunction<Tuple3<Double,StringValue,LongValue>>() {
				public Tuple3<Double, StringValue, LongValue> reduce(Tuple3<Double, StringValue, LongValue> value1, Tuple3<Double, StringValue, LongValue> value2) {
					return value1;
				}
			}).print();
			
			Plan p = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			
			ReduceOperatorBase<?, ?> reducer = (ReduceOperatorBase<?, ?>) sink.getInput();
			
			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());
			
			// check keys
			assertTrue(reducer.getKeyColumns(0) == null || reducer.getKeyColumns(0).length == 0);
			
			// DOP was not configured on the operator
			assertTrue(reducer.getDegreeOfParallelism() == 1 || reducer.getDegreeOfParallelism() == -1);
			
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
			final int DOP = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(DOP);
			
			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);
			
			initialData
				.groupBy(2)
				.reduce(new ReduceFunction<Tuple3<Double,StringValue,LongValue>>() {
					public Tuple3<Double, StringValue, LongValue> reduce(Tuple3<Double, StringValue, LongValue> value1, Tuple3<Double, StringValue, LongValue> value2) {
						return value1;
					}
				})
				.print();
			
			Plan p = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			
			ReduceOperatorBase<?, ?> reducer = (ReduceOperatorBase<?, ?>) sink.getInput();
			
			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());
			
			// DOP was not configured on the operator
			assertTrue(reducer.getDegreeOfParallelism() == DOP || reducer.getDegreeOfParallelism() == -1);
			
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
			final int DOP = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(DOP);
			
			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);
			
			initialData
				.groupBy(new KeySelector<Tuple3<Double,StringValue,LongValue>, StringValue>() {
					public StringValue getKey(Tuple3<Double, StringValue, LongValue> value) {
						return value.f1;
					}
				})
				.reduce(new ReduceFunction<Tuple3<Double,StringValue,LongValue>>() {
					public Tuple3<Double, StringValue, LongValue> reduce(Tuple3<Double, StringValue, LongValue> value1, Tuple3<Double, StringValue, LongValue> value2) {
						return value1;
					}
				}).setParallelism(4)
				.print();
			
			Plan p = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			
			
			MapOperatorBase<?, ?, ?> keyProjector = (MapOperatorBase<?, ?, ?>) sink.getInput();
			PlanUnwrappingReduceOperator<?, ?> reducer = (PlanUnwrappingReduceOperator<?, ?>) keyProjector.getInput();
			MapOperatorBase<?, ?, ?> keyExtractor = (MapOperatorBase<?, ?, ?>) reducer.getInput();
			
			// check the DOPs
			assertEquals(1, keyExtractor.getDegreeOfParallelism());
			assertEquals(4, reducer.getDegreeOfParallelism());
			assertEquals(4, keyProjector.getDegreeOfParallelism());
			
			// check types
			TypeInformation<?> keyValueInfo = new TupleTypeInfo<Tuple2<StringValue, Tuple3<Double,StringValue,LongValue>>>(
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
	private static final DataSet<Tuple3<Double, StringValue, LongValue>> getSourceDataSet(ExecutionEnvironment env) {
		return env.fromElements(new Tuple3<Double, StringValue, LongValue>(3.141592, new StringValue("foobar"), new LongValue(77)))
				.setParallelism(1);
	}
}
