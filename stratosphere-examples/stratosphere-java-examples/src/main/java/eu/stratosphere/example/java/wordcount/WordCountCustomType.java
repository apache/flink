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
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.util.Collector;


@SuppressWarnings("serial")
public class WordCountCustomType {
	
	public static class WC {
		
		public String word;
		public int count;
		
		public WC() {}

		public WC(String word, int count) {
			this.word = word;
			this.count = count;
		}
		
		@Override
		public String toString() {
			return "(" + word + ", " + count + ")";
		}
	}
	
	public static final class Tokenizer extends FlatMapFunction<String, WC> {
		
		@Override
		public void flatMap(String value, Collector<WC> out) {
			String[] tokens = value.toLowerCase().split("\\W");
			for (String token : tokens) {
				out.collect(new WC(token, 1));
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(4);
		
		DataSet<String> text = env.fromElements("To be", "or not to be", "or to be still", "and certainly not to be not at all", "is that the question?");
		
		DataSet<WC> tokenized = text.flatMap(new Tokenizer());

		
		DataSet<WC> result = tokenized
				
				.groupBy(new KeySelector<WC, String>() { public String getKey(WC v) { return v.word; } })
				
				.reduce(new ReduceFunction<WC>() {
					public WC reduce(WC value1, WC value2) {
						return new WC(value1.word, value1.count + value2.count);
					}
				});
		
		
		result.print();
		
		env.execute();
	}
}
