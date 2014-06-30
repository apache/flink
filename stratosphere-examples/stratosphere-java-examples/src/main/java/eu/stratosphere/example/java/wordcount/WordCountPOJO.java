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
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.example.java.wordcount.util.WordCountData;
import eu.stratosphere.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files.
 *
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 *
 * <p>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link eu.stratosphere.example.java.wordcount.util.WordCountData}.
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Stratosphere program.
 * <li>POJO data types with key expressions.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
@SuppressWarnings("serial")
public class WordCountPOJO {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		parseParameters(args);

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text = getTextDataSet(env);

		DataSet<WC> counts = text 
					.flatMap(new Tokenizer())
					.groupBy("word")
					.reduce(new ReduceFunction<WC>() {
							public WC reduce(WC value1, WC value2) {
								return new WC(value1.word, value1.count + value2.count);
							}
						});

		// emit result
		if(fileOutput) {
			counts.writeAsText(outputPath);
		} else {
			counts.print();
		}

		env.execute("WordCount with custom data types example");
	}

	// *************************************************************************
	//     USER DATA TYPES (POJOs)
	// *************************************************************************
	
	public static class WC {
		public String word;
		public int count;

		public WC() {
		}

		public WC(String word, int count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " " + count;
		}
	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple WC POJOs as "(word, 1)".
	 */
	public static final class Tokenizer extends FlatMapFunction<String, WC> {

		@Override
		public void flatMap(String value, Collector<WC> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new WC(token, 1));
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

	private static void parseParameters(String[] args) {

		if(args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Usage: WordCount <text path> <result path>");
				System.exit(1);
			}
		} else {
			System.out.println("Executing WordCount example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: WordCount <text path> <result path>");
		}
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

