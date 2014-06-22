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

package org.apache.flink.runtime.util.jartestprogram;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountWithAnonymousClass {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text = StaticData.getDefaultTextLineDataSet(env);

		DataSet<Tuple2<String, Integer>> counts =
			// split up the lines in pairs (2-tuples) containing: (word,1)
			text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
					// normalize and split the line
					String[] tokens = value.toLowerCase().split("\\W+");

					// emit the pairs
					for (String token : tokens) {
						if (token.length() > 0) {
							out.collect(new Tuple2<String, Integer>(token, 1));
						}
					}
				}
			})
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// emit result
		counts.print();

		// execute program
		env.execute("WordCount Example");
	}
}
