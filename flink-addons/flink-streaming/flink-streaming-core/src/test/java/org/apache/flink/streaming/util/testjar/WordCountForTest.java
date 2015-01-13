/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.testjar;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;


public class WordCountForTest {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public JobGraph getJobGraph(String[] args) {
		if (args.length < 2) {
			throw new IllegalArgumentException("Missing parameters");
		}

		textPath = args[0];
		outputPath = args[1];

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> text = env.readTextFile(textPath);

		DataStream<Tuple2<String, Integer>> counts =
			// split up the lines in pairs (2-tuples) containing: (word,1)
			text.flatMap(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0).sum(1);

		// emit result
		counts.writeAsText(outputPath, 1);

		return env.getJobGraphBuilder().getJobGraph();
	}


	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" (Tuple2<String,
	 * Integer>).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String inTuple, Collector<Tuple2<String, Integer>> out)
			throws Exception {
			// tokenize the line
			StringTokenizer tokenizer = new StringTokenizer(inTuple);

			// emit the pairs
			while (tokenizer.hasMoreTokens()) {
				out.collect(new Tuple2<String, Integer>(tokenizer.nextToken(), 1));
			}
		}
	}

	private static String textPath;
	private static String outputPath;


}
