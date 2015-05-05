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

package org.apache.flink.examples.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files. 
 * 
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 * 
 * <p>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions. 
 * </ul>
 * 
 */
@SuppressWarnings("serial")
public class WordCount {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}
		
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// get input data
		DataSet<String> text = getTextDataSet(env);

		DataSet<Tuple2<String, Integer>> counts = 
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// emit result
		if(fileOutput) {
			counts.writeAsText(outputPath);
			// execute program
			env.execute("WordCount Example");
		} else {
			counts.print();
		}
		

	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************
	
	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into 
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

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
	
	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	
	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;
	
	private static boolean parseParameters(String[] args) {
		
		if(args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Usage: WordCount <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing WordCount example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: WordCount <text path> <result path>");
		}
		return true;
	}
	
	private static DataSet<String> getTextDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			// read the text file from given input path
			return env.readTextFile(textPath);
		} else {
			// get default test text data
			return WordCountData.getDefaultTextLineDataSet(env);
		}
	}
}
