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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.base.GenericDataSinkBase;
import eu.stratosphere.api.common.operators.base.GenericDataSourceBase;
import eu.stratosphere.api.common.operators.base.GroupReduceOperatorBase;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.ValueTypeInfo;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.TypeInformation;

@SuppressWarnings("serial")
public class DistinctTranslationTests implements java.io.Serializable {

	@Test
	public void translateDistinctPlain() {
		try {
			final int DOP = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(DOP);
			
			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);
			
			initialData.distinct().print();
			
			Plan p = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			
			// currently distinct is translated to a GroupReduce
			GroupReduceOperatorBase<?, ?, ?> reducer = (GroupReduceOperatorBase<?, ?, ?>) sink.getInput();
			
			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());
			
			// check keys
			assertArrayEquals(new int[] {0, 1, 2}, reducer.getKeyColumns(0));
			
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
	public void translateDistinctPlain2() {
		try {
			final int DOP = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(DOP);
			
			DataSet<CustomType> initialData = getSourcePojoDataSet(env);
			
			initialData.distinct().print();
			
			Plan p = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			
			// currently distinct is translated to a GroupReduce
			GroupReduceOperatorBase<?, ?, ?> reducer = (GroupReduceOperatorBase<?, ?, ?>) sink.getInput();
			
			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());
			
			// check keys
			assertArrayEquals(new int[] {0}, reducer.getKeyColumns(0));
			
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
	public void translateDistinctPosition() {
		try {
			final int DOP = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(DOP);
			
			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);
			
			initialData.distinct(1, 2).print();
			
			Plan p = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			
			// currently distinct is translated to a GroupReduce
			GroupReduceOperatorBase<?, ?, ?> reducer = (GroupReduceOperatorBase<?, ?, ?>) sink.getInput();
			
			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());
			
			// check keys
			assertArrayEquals(new int[] {1, 2}, reducer.getKeyColumns(0));
			
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
	public void translateDistinctKeySelector() {
		try {
			final int DOP = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(DOP);
			
			DataSet<Tuple3<Double, StringValue, LongValue>> initialData = getSourceDataSet(env);
			
			initialData.distinct(new KeySelector<Tuple3<Double,StringValue,LongValue>, StringValue>() {
				public StringValue getKey(Tuple3<Double, StringValue, LongValue> value) {
					return value.f1;
				}
			}).setParallelism(4).print();
			
			Plan p = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			
			PlanUnwrappingReduceGroupOperator<?, ?, ?> reducer = (PlanUnwrappingReduceGroupOperator<?, ?, ?>) sink.getInput();
			MapOperatorBase<?, ?, ?> keyExtractor = (MapOperatorBase<?, ?, ?>) reducer.getInput();
			
			// check the DOPs
			assertEquals(1, keyExtractor.getDegreeOfParallelism());
			assertEquals(4, reducer.getDegreeOfParallelism());
			
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
			final int DOP = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(DOP);
			
			DataSet<CustomType> initialData = getSourcePojoDataSet(env);
			
			initialData.distinct("myInt").print();
			
			Plan p = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			
			// currently distinct is translated to a GroupReduce
			GroupReduceOperatorBase<?, ?, ?> reducer = (GroupReduceOperatorBase<?, ?, ?>) sink.getInput();
			
			// check types
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getInputType());
			assertEquals(initialData.getType(), reducer.getOperatorInfo().getOutputType());
			
			// check keys
			assertArrayEquals(new int[] {0}, reducer.getKeyColumns(0));
			
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
	
	private static class CustomType implements Serializable {
		
		private static final long serialVersionUID = 1L;
		public int myInt;
		@SuppressWarnings("unused")
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
