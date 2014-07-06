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
 * Implements a "WordCount" program that computes a simple word occurrence histogram
 * over a hard coded example or text files. This example demonstrates how to use KeySelectors, ReduceFunction and FlatMapFunction.
 */
@SuppressWarnings("serial")
public class WordCountKeySelector {
	
	/**
	 * Runs the WordCount program.
	 * 
	 * @param args Input and output file.
	 */
	public static void main(String[] args) throws Exception {
		// Check whether arguments are given and tell user how to use this example with files.
		if (args.length < 2) {
			System.out.println("You can specify: WordCountKeySelector <input path> <result path>, in order to work with files.");
		}
		
		// Input and output path [optional].
		String inputPath = null;
		String outputPath = null;
		
		// Get the environment as starting point.
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// Read the text file from given input path or use a hard coded example.
		DataSet<String> text = null;
		if(args.length >= 1) {
			inputPath = args[0];
			text = env.readTextFile(inputPath);
		}
		else {
			System.out.println("No input file specified. Using hard coded example.");
			text = env.fromElements("To be", "or not to be", "or to be still", "and certainly not to be not at all", "is that the question?");
		}
		
		// Split up the lines in CustomizedWord containing: (word,1)
		DataSet<CustomizedWord> words = text.flatMap(new Tokenizer());
		
		// Group by the tuple field "0" and sum up tuple field "1". Create KeySelector to be able to group CustomizedWord. Instantiate customized reduce function. 
		DataSet<CustomizedWord> result = words.groupBy(new CustomizedWordKeySelector())
				.reduce(new CustomizedWordReducer());
		
		// Write result into text file if output file is specified. Otherwise print to console.
		if( args.length >= 2) {
			outputPath = args[1];
			// Write out the result
			result.writeAsText(outputPath);
		}
		else {
			System.out.println("No output file specified. Printing result to console.");
			// Print result to console
			result.print();
		}
		
		// Execute the defined program
		env.execute("Word Count");
	}
	
	/**
	 * Implements a string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into 
	 * multiple pairs in the form of CustomizedWord(word,1) ).
	 */
	public static final class Tokenizer extends FlatMapFunction<String, CustomizedWord> {

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
	 * Customized reducer for CustomizedWord. When the keys of two CustomizedWord classes are the same,
	 * both are reduced into one CustomizedWord class.
	 */
	public static class CustomizedWordReducer extends ReduceFunction<CustomizedWord>{

		/**
		 * This method is applied to all members of a group. Hence to all CustomizedWord instances which have 
		 * the same key. One CustomizedWord instance is returned in which the count is increased by one.
		 */
		@Override
		public CustomizedWord reduce(CustomizedWord value1,
				CustomizedWord value2) throws Exception {
			value2.count += value1.count;
			return value2;
		}
		
	}
	
	/**
	 * This class is a customized word and count class. It represents a Tuple with two entries (word,count). 
	 * For this example a customized class is used in order to show how to use KeySelectors.
	 */
	public static class CustomizedWord{
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
		 * @param word The word.
		 * @param count The number of appearances.
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
			return "<"+word+","+count+">";
		}
	}
	
	/**
	 * KeySelector written for CustomizedWord.
	 * This KeySelector extracts the KEY out of CustomizedWord.
	 * 
	 */
	public static class CustomizedWordKeySelector extends KeySelector<CustomizedWord, String> {
		
		/**
		 * This method is called in order to extract the KEY out of CustomizedWord.
		 */
		@Override
		public String getKey(CustomizedWord value) {
			// Return the word (String), which is key.
			return value.word;
		}
		
	}
}
