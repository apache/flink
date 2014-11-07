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

package org.apache.flink.test.broadcastvars;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;

@SuppressWarnings("serial")
public class BroadcastVarInitializationITCase extends JavaProgramTestBase {
	
	@Override
	protected void testProgram() throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(4);
		
		DataSet<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
		
		IterativeDataSet<Integer> iteration = data.iterate(10);
		
		DataSet<Integer> result = data.reduceGroup(new PickOneAllReduce()).withBroadcastSet(iteration, "bc");
		
		final List<Integer> resultList = new ArrayList<Integer>();
		iteration.closeWith(result).output(new LocalCollectionOutputFormat<Integer>(resultList));
		
		env.execute();
		
		Assert.assertEquals(8, resultList.get(0).intValue());
	}

	
	public static class PickOneAllReduce extends RichGroupReduceFunction<Integer, Integer> {
		
		private Integer bcValue;
		
		@Override
		public void open(Configuration parameters) {
			this.bcValue = getRuntimeContext().getBroadcastVariableWithInitializer("bc", new PickFirstInitializer());
		}

		@Override
		public void reduce(Iterable<Integer> records, Collector<Integer> out) {
			if (bcValue == null) {
				return;
			}
			final int x = bcValue;
			
			for (Integer y : records) { 
				if (y > x) {
					out.collect(y);
					return;
				}
			}

			out.collect(bcValue);
		}
	}
	
	public static class PickFirstInitializer implements BroadcastVariableInitializer<Integer, Integer> {

		@Override
		public Integer initializeBroadcastVariable(Iterable<Integer> data) {
			Iterator<Integer> iter = data.iterator();
			return iter.hasNext() ? iter.next() : null;
		}
	}
}
