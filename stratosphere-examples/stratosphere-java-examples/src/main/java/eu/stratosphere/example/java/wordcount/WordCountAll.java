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
package eu.stratosphere.example.java.wordcount;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;


public class WordCountAll {
	
	public static final class Tokenizer extends MapFunction<String, Integer> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public Integer map(String value) {
			String[] tokens = value.toLowerCase().split("\\W");
			return Integer.valueOf(tokens.length);
		}
	}
	
	public static final class Counter extends ReduceFunction<Integer> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce(Integer val1, Integer val2) {
			return val1 + val2;
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);
		
		DataSet<String> text = env.fromElements("To be", "or not to be", "or to be still", "and certainly not to be not at all", "is that the question?");
		
		DataSet<Integer> result = text.map(new Tokenizer()).reduce(new Counter());
				
		result.print();
		
//		System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
