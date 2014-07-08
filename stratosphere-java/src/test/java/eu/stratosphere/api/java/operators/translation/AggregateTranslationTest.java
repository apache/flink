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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.base.GenericDataSinkBase;
import eu.stratosphere.api.common.operators.base.GenericDataSourceBase;
import eu.stratosphere.api.common.operators.base.GroupReduceOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.types.StringValue;

public class AggregateTranslationTest {

	@Test
	public void translateAggregate() {
		try {
			final int DOP = 8;
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(DOP);
			
			@SuppressWarnings("unchecked")
			DataSet<Tuple3<Double, StringValue, Long>> initialData = 
					env.fromElements(new Tuple3<Double, StringValue, Long>(3.141592, new StringValue("foobar"), new Long(77)));
			
			initialData.groupBy(0).aggregate(Aggregations.MIN, 1).and(Aggregations.SUM, 2).print();
			
			Plan p = env.createProgramPlan();
			
			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			
			GroupReduceOperatorBase<?, ?, ?> reducer = (GroupReduceOperatorBase<?, ?, ?>) sink.getInput();
			
			// check keys
			assertEquals(1, reducer.getKeyColumns(0).length);
			assertEquals(0, reducer.getKeyColumns(0)[0]);
			
			assertEquals(-1, reducer.getDegreeOfParallelism());
			assertTrue(reducer.isCombinable());
			
			assertTrue(reducer.getInput() instanceof GenericDataSourceBase<?, ?>);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test caused an error: " + e.getMessage());
		}
	}
}
