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

package org.apache.flink.examples.java.misc;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over some sample data and collects the results with an
 * implementation of a {@link RemoteCollectorConsumer}.
 */
@SuppressWarnings("serial")
public class RemoteCollectorOutputFormatExample {

	public static void main(String[] args) throws Exception {

		/**
		 * We create a remote {@link ExecutionEnvironment} here, because this
		 * OutputFormat is designed for use in a distributed setting. For local
		 * use you should consider using the {@link LocalCollectionOutputFormat
		 * <T>}.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("<remote>", 6124,
				"/path/to/your/file.jar");

		// get input data
		DataSet<String> text = env.fromElements(
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,");

		DataSet<Tuple2<String, Integer>> counts =
		// split up the lines in pairs (2-tuples) containing: (word,1)
		text.flatMap(new LineSplitter())
		// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0).aggregate(Aggregations.SUM, 1);

		// emit result
		RemoteCollectorImpl.collectLocal(counts,
				new RemoteCollectorConsumer<Tuple2<String, Integer>>() {
					// user defined IRemoteCollectorConsumer
					@Override
					public void collect(Tuple2<String, Integer> element) {
						System.out.println("word/occurrences:" + element);
					}
				});

		// local collection to store results in
		Set<Tuple2<String, Integer>> collection = new HashSet<Tuple2<String, Integer>>();
		// collect results from remote in local collection
		RemoteCollectorImpl.collectLocal(counts, collection);

		// execute program
		env.execute("WordCount Example with RemoteCollectorOutputFormat");

		System.out.println(collection);
		
		RemoteCollectorImpl.shutdownAll();
	}

	//
	// User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" (Tuple2<String,
	 * Integer>).
	 */
	public static final class LineSplitter implements
			FlatMapFunction<String, Tuple2<String, Integer>> {

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
