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

/**
 * Implements the "WordCount" program which takes a collection of strings and counts the number of
 * occurrences of each word in these strings. Finally, the result will be written to the
 * console.
 */
@SuppressWarnings("serial")
public class WordCountCollection {
	
	/**
	 * Runs the WordCount program. 
	 * 
	 * @param args Command line parameters, ignored.
	 */
	public static void main(String[] args) throws Exception {
		
		// get the environment as starting point
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// define the strings to be analyzed
		DataSet<String> text = env.fromElements("To be", "or not to be", "or to be still", "and certainly not to be not at all", "is that the question?");
		
		// split the strings into tuple of (word,1), group by field "0" and sum up field "1" 
		DataSet<Tuple2<String, Integer>> result = text.flatMap(new Tokenizer()).groupBy(0).aggregate(SUM, 1);
		
		// print the result on the console
		result.print();
		
		// execute the defined program
		env.execute();
	}
	
	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into 
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class Tokenizer extends FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");
			
			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
