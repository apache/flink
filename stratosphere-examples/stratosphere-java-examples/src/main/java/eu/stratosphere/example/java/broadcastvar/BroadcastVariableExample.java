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
package eu.stratosphere.example.java.broadcastvar;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;


/**
 *
 */
@SuppressWarnings("serial")
public class BroadcastVariableExample {

	public static class MultiplyingMapper extends FlatMapFunction<String, String> {

		private int factor; 
		
		@Override
		public void open(Configuration parameters) throws Exception {
			factor = getRuntimeContext().<Integer>getBroadcastVariable("FACTOR").iterator().next();
		}
		
		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {
			for (int i = 0; i < factor; i++) {
				out.collect(value);
			}
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> strings = env.fromElements("This", "example", "is", "what", "happens", "when", "creativity", "runs", "low", "at", "the", "end", "of", "the", "day");
		
		DataSet<Integer> count = env.fromElements(5);
		
		strings.flatMap(new MultiplyingMapper()).name("multplier").withBroadcastSet(count, "FACTOR").print();
		
		env.execute("Peter's job");
	}

}
