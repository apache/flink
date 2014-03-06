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
import eu.stratosphere.api.java.tuple.*;
import eu.stratosphere.util.Collector;

import static eu.stratosphere.api.java.aggregation.Aggregations.*;


public class WordCountAggregator {
	
	public static final class Tokenizer extends FlatMapFunction<String, Tuple2<String, Integer>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W");
			for (String token : tokens) {
				out.collect(new Tuple2<String, Integer>(token, 1));
			}
		}
	}
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: <input path> <output path>");
			return;
		}
		
		final String inputPath = args[0];
		final String outputPath = args[1];
		
		final ExecutionEnvironment context = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> text = context.readTextFile(inputPath);
		
		DataSet<Tuple2<String, Integer>> result = text.flatMap(new Tokenizer()).groupBy(0).aggregate(SUM, 1);
		
		result.writeAsText(outputPath);
	}
}
