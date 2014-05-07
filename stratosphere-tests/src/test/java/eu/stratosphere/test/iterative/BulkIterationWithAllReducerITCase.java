/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.test.iterative;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.io.LocalCollectionOutputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.util.JavaProgramTestBase;
import eu.stratosphere.util.Collector;


@SuppressWarnings("serial")
public class BulkIterationWithAllReducerITCase extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);
		
		DataSet<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
		
		IterativeDataSet<Integer> iteration = data.iterate(10);
		
		DataSet<Integer> result = data.reduceGroup(new PickOneAllReduce()).withBroadcastSet(iteration, "bc");
		
		final List<Integer> resultList = new ArrayList<Integer>();
		iteration.closeWith(result).output(new LocalCollectionOutputFormat<Integer>(resultList));
		
		env.execute();
		
		Assert.assertEquals(8, resultList.get(0).intValue());
	}

	
	public static class PickOneAllReduce extends GroupReduceFunction<Integer, Integer> {
		
		private Integer bcValue;
		
		@Override
		public void open(Configuration parameters) {
			Collection<Integer> bc = getRuntimeContext().getBroadcastVariable("bc");
			this.bcValue = bc.isEmpty() ? null : bc.iterator().next();
		}

		@Override
		public void reduce(Iterator<Integer> records, Collector<Integer> out) {
			if (bcValue == null) {
				return;
			}
			final int x = bcValue;
			
			while (records.hasNext()) { 
				int y = records.next();

				if (y > x) {
					out.collect(y);
					return;
				}
			}

			out.collect(bcValue);
		}
	}
}