/**
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

package org.apache.flink.api.common.operators;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.junit.Test;

@SuppressWarnings("serial")
public class CollectionExecutionIterationTest implements java.io.Serializable {

	@Test
	public void testBulkIteration() {
		try {
			ExecutionEnvironment env = new CollectionEnvironment();
			
			IterativeDataSet<Integer> iteration = env.fromElements(1).iterate(10);
			
			DataSet<Integer> result = iteration.closeWith(iteration.map(new AddSuperstepNumberMapper()));
			
			List<Integer> collected = new ArrayList<Integer>();
			result.output(new LocalCollectionOutputFormat<Integer>(collected));
			
			env.execute();
			
			assertEquals(1, collected.size());
			assertEquals(56, collected.get(0).intValue());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testBulkIterationWithTerminationCriterion() {
		try {
			ExecutionEnvironment env = new CollectionEnvironment();
			
			IterativeDataSet<Integer> iteration = env.fromElements(1).iterate(100);
			
			DataSet<Integer> iterationResult = iteration.map(new AddSuperstepNumberMapper());

			DataSet<Integer> terminationCriterion = iterationResult.filter(new FilterFunction<Integer>() {
				public boolean filter(Integer value) {
					return value < 50;
				}
			});
			
			List<Integer> collected = new ArrayList<Integer>();
			
			iteration.closeWith(iterationResult, terminationCriterion)
					.output(new LocalCollectionOutputFormat<Integer>(collected));
			
			env.execute();
			
			assertEquals(1, collected.size());
			assertEquals(56, collected.get(0).intValue());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public static class AddSuperstepNumberMapper extends RichMapFunction<Integer, Integer> {
		
		@Override
		public Integer map(Integer value) {
			int superstep = getIterationRuntimeContext().getSuperstepNumber();
			return value + superstep;
		}
	}
}
