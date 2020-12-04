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

package org.apache.flink.connector.hbase1.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.example.HBaseFlinkTestConstants;
import org.apache.flink.util.Collector;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * Simple stub for HBase DataSet write
 *
 * <p>To run the test first create the test table with hbase shell.
 *
 * <p>Use the following commands:
 * <ul>
 *     <li>create 'test-table', 'someCf'</li>
 * </ul>
 *
 */
@SuppressWarnings("serial")
public class HBaseWriteExample {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text = getTextDataSet(env);

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word, 1)
				text.flatMap(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// emit result
		Job job = Job.getInstance();
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTableName);
		// TODO is "mapred.output.dir" really useful?
		job.getConfiguration().set("mapred.output.dir", HBaseFlinkTestConstants.TMP_DIR);
		counts.map(new RichMapFunction <Tuple2<String, Integer>, Tuple2<Text, Mutation>>() {
			private transient Tuple2<Text, Mutation> reuse;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				reuse = new Tuple2<Text, Mutation>();
			}

			@Override
			public Tuple2<Text, Mutation> map(Tuple2<String, Integer> t) throws Exception {
				reuse.f0 = new Text(t.f0);
				Put put = new Put(t.f0.getBytes(ConfigConstants.DEFAULT_CHARSET));
				put.add(HBaseFlinkTestConstants.CF_SOME, HBaseFlinkTestConstants.Q_SOME, Bytes.toBytes(t.f1));
				reuse.f1 = put;
				return reuse;
			}
		}).output(new HadoopOutputFormat<Text, Mutation>(new TableOutputFormat<Text>(), job));

		// execute program
		env.execute("WordCount (HBase sink) Example");
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word, 1)" (Tuple2&lt;String, Integer&gt;).
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
	private static String outputTableName = HBaseFlinkTestConstants.TEST_TABLE_NAME;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 2) {
				textPath = args[0];
				outputTableName = args[1];
			} else {
				System.err.println("Usage: HBaseWriteExample <text path> <output table>");
				return false;
			}
		} else {
			System.out.println("Executing HBaseWriteExample example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: HBaseWriteExample <text path> <output table>");
		}
		return true;
	}

	private static DataSet<String> getTextDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			// read the text file from given input path
			return env.readTextFile(textPath);
		} else {
			// get default test text data
			return getDefaultTextLineDataSet(env);
		}
	}

	private static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {
		return env.fromElements(WORDS);
	}

	private static final String[] WORDS = new String[] {
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
	};
}
