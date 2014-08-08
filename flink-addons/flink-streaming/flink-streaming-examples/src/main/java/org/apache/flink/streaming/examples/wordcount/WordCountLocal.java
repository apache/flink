/**
 *
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
 *
 */

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestDataUtil;

public class WordCountLocal {

	// This example will count the occurrence of each word in the input file.

	public static void main(String[] args) {

		TestDataUtil.downloadIfNotExists("hamlet.txt");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		DataStream<Tuple2<String, Integer>> dataStream = env
				.readTextFile("src/test/resources/testdata/hamlet.txt")
				.flatMap(new WordCountSplitter())
				.groupBy(0)
				.reduce(new WordCountCounter());

		dataStream.print();

		env.execute();
	}
}
