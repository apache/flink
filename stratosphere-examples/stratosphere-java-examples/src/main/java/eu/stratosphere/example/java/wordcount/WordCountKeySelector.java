/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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
import eu.stratosphere.example.java.wordcount.util.WordCountData;
import eu.stratosphere.util.Collector;

/**
 * Implements a "WordCount" program that computes a simple word occurrence
 * histogram over a hard coded example or text files. This example demonstrates
 * how to use KeySelectors, ReduceFunction and FlatMapFunction.
 * 
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 * 
 * <p>
 * Usage:
 * <code>WordCountKeySelector &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 * 
 * *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Stratosphere program.
 * <li>write and use user-defined functions such as KeySelectors, ReduceFunction
 * and FlatMapFunction.
 * </ul>
 * 
 */
@SuppressWarnings("serial")
public class WordCountKeySelector {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	/**
	 * Runs the WordCount program.
	 * @param args Arguments given, input and output file/path.
	 */
	public static void main(String[] args) throws Exception {
		// Check whether arguments are given and parse them. Tell user how to
		// use this example if not all arguments are specified.
		parseParameters(args);

		// Get the environment - to be able to access files.
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		// Get input data - here the execution environment is used to read from
		// file.
		DataSet<String> text = getTextDataSet(env);

		DataSet<CustomizedWord> result =
				// Split up the lines in CustomizedWord containing: (word,1)
				text.flatMap(new Tokenizer())
				// Group words using a custom KeySelector. Groups will contain equal words.
				.groupBy(new CustomizedWordKeySelector())
				// Reduce groups using a customized reducer. All words in a
				// group are reduced to one word and the quantity(n) is stored
				// like (word,n).
				.reduce(new CustomizedWordReducer());

		// Output result, dependent on which arguments were given.
		if (fileOutput) {
			// Write to file
			result.writeAsText(outputPath);
		} else {
			// Print to console
			result.print();
		}

		// Execute the predefined program
		env.execute("Word Count");
	}

	// *************************************************************************
	// USER FUNCTIONS 
	// *************************************************************************

	/**
	 * Implements a string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of CustomizedWord(word,1) ).
	 */
	public static final class Tokenizer extends
			FlatMapFunction<String, CustomizedWord> {

		@Override
		public void flatMap(String value, Collector<CustomizedWord> out) {
			// Normalize (convert words to lower case, so that e.g. "Hello"
			// and "hello" become the same) and split the line.
			String[] tokens = value.toLowerCase().split("\\W+");

			// Emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new CustomizedWord(token, 1));
				}
			}
		}
	}

	/**
	 * Customized reducer for CustomizedWord. When the keys of two
	 * CustomizedWord classes are the same, both are reduced into one
	 * CustomizedWord class.
	 */
	public static class CustomizedWordReducer extends
			ReduceFunction<CustomizedWord> {

		/**
		 * This method is applied to all members of a group. Hence to all
		 * CustomizedWord instances which have the same key. One CustomizedWord
		 * instance is returned in which the count is increased by one.
		 */
		@Override
		public CustomizedWord reduce(CustomizedWord value1,
				CustomizedWord value2) throws Exception {
			value2.count += value1.count;
			return value2;
		}

	}

	/**
	 * This class is a customized word and count class. It represents a tuple
	 * with two entries (word,count). For this example a customized class is
	 * used in order to show how to use KeySelectors.
	 */
	public static class CustomizedWord {
		// Word
		public String word;
		// Count of how often word was found
		public int count;

		/**
		 * Standard constructor.
		 */
		public CustomizedWord() {
		}

		/**
		 * Constructor to set public members of class.
		 * 
		 * @param word
		 *            The word.
		 * @param count
		 *            The number of appearances.
		 */
		public CustomizedWord(String word, int count) {

			// Set values
			this.word = word;
			this.count = count;
		}

		/**
		 * Convert to String. For a nice printed result.
		 */
		@Override
		public String toString() {
			return "<" + word + "," + count + ">";
		}
	}

	/**
	 * KeySelector written for CustomizedWord. This KeySelector extracts the KEY
	 * out of CustomizedWord.
	 * 
	 */
	public static class CustomizedWordKeySelector extends
			KeySelector<CustomizedWord, String> {

		/**
		 * This method is called in order to extract the KEY out of
		 * CustomizedWord.
		 */
		@Override
		public String getKey(CustomizedWord value) {
			// Return the word (String), which is key.
			return value.word;
		}

	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************
	
	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;

	/**
	 * Parses parameters and stores input and output path.
	 * 
	 * @param args
	 */
	private static void parseParameters(String[] args) {
		System.out.println("Executing word count example.");
		// Check if input file is specified
		if (args.length >= 1) {
			textPath = args[0];
			System.out.println("Input from file: " + textPath);
		} else {
			System.out
					.println("No input file specified. Using hard coded example.");
		}

		// Check for output
		if (args.length >= 2) {
			outputPath = args[1];
			System.out.println("Output to file: " + outputPath);
			fileOutput = true;
		} else {
			System.out
					.println("No output file specified. Printing result to console.");
		}

		// Not all arguments given, print usage.
		if (args.length < 2) {
			System.out
					.println("Usage: WordCountKeySelector <input path> <result path>.");
		}
	}

	private static DataSet<String> getTextDataSet(ExecutionEnvironment env) {
		if (textPath != null && env != null) {
			// Read the text file from given input path
			return env.readTextFile(textPath);
		} else {
			// Get default test text data
			return WordCountData.getDefaultTextLineDataSet(env);
		}
	}

}
