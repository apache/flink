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
package eu.stratosphere.client.testjar;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

/**
 * Wordcount for placing at least something into the jar file.
 * 
 */
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
				.aggregate(Aggregations.SUM, 1);

		// emit result
		if(fileOutput) {
			counts.writeAsCsv(outputPath, "\n", " ");
		} else {
			counts.print();
		}
		
		// execute program
		env.execute("WordCount Example");
	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************
	
	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into 
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class Tokenizer extends FlatMapFunction<String, Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;

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
			return env.fromElements(
					"To be, or not to be,--that is the question:--",
					"Whether 'tis nobler in the mind to suffer",
					"The slings and arrows of outrageous fortune",
					"Or to take arms against a sea of troubles,",
					"And by opposing end them?--To die,--to sleep,--",
					"No more; and by a sleep to say we end",
					"The heartache, and the thousand natural shocks",
					"That flesh is heir to,--'tis a consummation",
					"Devoutly to be wish'd. To die,--to sleep;--",
					"To sleep! perchance to dream:--ay, there's the rub;",
					"For in that sleep of death what dreams may come,",
					"When we have shuffled off this mortal coil,",
					"Must give us pause: there's the respect",
					"That makes calamity of so long life;",
					"For who would bear the whips and scorns of time,",
					"The oppressor's wrong, the proud man's contumely,",
					"The pangs of despis'd love, the law's delay,",
					"The insolence of office, and the spurns",
					"That patient merit of the unworthy takes,",
					"When he himself might his quietus make",
					"With a bare bodkin? who would these fardels bear,",
					"To grunt and sweat under a weary life,",
					"But that the dread of something after death,--",
					"The undiscover'd country, from whose bourn",
					"No traveller returns,--puzzles the will,",
					"And makes us rather bear those ills we have",
					"Than fly to others that we know not of?",
					"Thus conscience does make cowards of us all;",
					"And thus the native hue of resolution",
					"Is sicklied o'er with the pale cast of thought;",
					"And enterprises of great pith and moment,",
					"With this regard, their currents turn awry,",
					"And lose the name of action.--Soft you now!",
					"The fair Ophelia!--Nymph, in thy orisons",
					"Be all my sins remember'd."
					);
		}
	}
}