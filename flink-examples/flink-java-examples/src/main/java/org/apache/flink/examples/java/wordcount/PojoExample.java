/**
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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;


/**
 * This example shows an implementation of Wordcount without using the
 * Tuple2 type, but a custom class.
 *
 */
@SuppressWarnings("serial")
public class PojoExample {
	
	/**
	 * This is the POJO (Plain Old Java Object) that is being used
	 * for all the operations.
	 * As long as all fields are public or have a getter/setter, the system can handle them
	 */
	public static class Word {
		// fields
		private String word;
		private Integer frequency;
		
		// constructors
		public Word() {
		}
		public Word(String word, int i) {
			this.word = word;
			this.frequency = i;
		}
		// getters setters
		public String getWord() {
			return word;
		}
		public void setWord(String word) {
			this.word = word;
		}
		public Integer getFrequency() {
			return frequency;
		}
		public void setFrequency(Integer frequency) {
			this.frequency = frequency;
		}
		// to String
		@Override
		public String toString() {
			return "Word="+word+" freq="+frequency;
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		if(!parseParameters(args)) {
			return;
		}
		
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// get input data
		DataSet<String> text = getTextDataSet(env);
		
		DataSet<Word> counts = 
			// split up the lines into Word objects (with frequency = 1)
			text.flatMap(new Tokenizer())
			// group by the field word and sum up the frequency
			.groupBy("word")
			.reduce(new ReduceFunction<Word>() {
				@Override
				public Word reduce(Word value1, Word value2) throws Exception {
					return new Word(value1.word,value1.frequency + value2.frequency);
				}
			});
		
		if(fileOutput) {
			counts.writeAsText(outputPath, WriteMode.OVERWRITE);
			// execute program
			env.execute("WordCount-Pojo Example");
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
	public static final class Tokenizer implements FlatMapFunction<String, Word> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Word> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");
			
			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Word(token, 1));
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
