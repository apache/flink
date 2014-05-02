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

import static eu.stratosphere.api.java.aggregation.Aggregations.SUM;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

@SuppressWarnings("serial")
public class WordCountCollection {
	
	public static final class Tokenizer extends FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W");
			for (String token : tokens) {
				out.collect(new Tuple2<String, Integer>(token, 1));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> text = env.fromElements("To be", "or not to be", "or to be still", "and certainly not to be not at all", "is that the question?");
		
		DataSet<Tuple2<String, Integer>> result = text.flatMap(new Tokenizer()).groupBy(0).aggregate(SUM, 1);
				
		result.print();
		env.execute();
	}
}
