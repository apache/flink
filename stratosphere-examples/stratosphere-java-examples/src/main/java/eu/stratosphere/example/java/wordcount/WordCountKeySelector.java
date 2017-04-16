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
import eu.stratosphere.util.Collector;

/**
 * Implements a "WordCount" program that computes a simple word occurrence
 * histogram over a hard coded example or text files. This example demonstrates
 * how to use KeySelectors, ReduceFunction and FlatMapFunction.
 */
@SuppressWarnings("serial")
public class WordCountKeySelector {

	/**
	 * Runs the WordCount program.
	 * @param args Arguments given, input and output path.
	 */
	public static void main(String[] args) throws Exception {
		// Check whether arguments are given and tell user how to use this example.
		printUsageIfNeeded(args);
		
		// Get the environment - to be able to access files.
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		
		// Get input data - here the execution environment is used to read from file.
		DataSet<String> text = getTextFromInput(args, env);

		// Split up the lines in CustomizedWord containing: (word,1)
		DataSet<CustomizedWord> words = text.flatMap(new Tokenizer());

		// Group by the tuple field "0" and sum up tuple field "1". Create
		// KeySelector to be able to group CustomizedWord. Instantiate
		// customized reduce function.
		DataSet<CustomizedWord> result = words.groupBy(
				new CustomizedWordKeySelector()).reduce(
				new CustomizedWordReducer());

		// Output result, dependent on which arguments were given.
		printOutput(args, result);
		
		// Execute the defined program
		env.execute("Word Count");
	}
	
	/**
	 * Handles arguments given by the user and returns the text from a specified input file
	 * or from a hard coded example if no file is specified.
	 * @param args Arguments given.
	 * @param env Environment of execution.
	 * @return Null if args is null or if args has a set input path and env is null. Otherwise it returns
	 * a DataSet with the file input (when input path is specified) or a hard coded example (no input path specified).
	 */
	private static DataSet<String> getTextFromInput(String[] args,
			ExecutionEnvironment env) {
		if(args == null)
			return null;
		// Read the text file from given input path or use a hard coded example.
		if (args.length >= 1) {
			String inputPath = args[0];
			if(env != null) {
			return env.readTextFile(inputPath);
			}else {
				return null;
			}
		} else {
			System.out
					.println("No input file specified. Using hard coded example.");
			return env.fromElements("To be", "or not to be", "or to be still",
					"and certainly not to be not at all",
					"is that the question?");
		}
	}
	
	/**
	 * Prints the result either to a file or to standard output (if no output file is specified).
	 * @param args Arguments given.
	 * @param result Result to print.
	 */
	private static void printOutput(String[] args,
			DataSet<CustomizedWord> result) {
		if(result == null || args == null)
			return;
		
		// print to console.
		if (args.length >= 2 ) {
			String outputPath = args[1];
			// Write out the result
			result.writeAsText(outputPath);
		} else {
			System.out
					.println("No output file specified. Printing result to console.");
			// Print result to console
			result.print();
		}

	}
	
	/**
	 * Checks whether more two or more arguments are specified. If not usage pattern is printed to standard output.
	 * @param args Arguments given.
	 */
	private static void printUsageIfNeeded(String[] args) {
		if (args.length < 2) {
			System.out
					.println("You can specify: WordCountKeySelector <input path> <result path>, in order to work with files.");
		}
	}

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
	 * This class is a customized word and count class. It represents a Tuple
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
}
