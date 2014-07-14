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

package org.apache.flink.streaming.examples.window.wordcount;

import org.apache.flink.streaming.api.DataStream;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestDataUtil;

import org.apache.flink.api.java.tuple.Tuple3;

public class WindowWordCountLocal {

	private static final int PARALLELISM = 1;

	// This example will count the occurrence of each word in the input file with a sliding window.
	
	public static void main(String[] args) {
		
		TestDataUtil.downloadIfNotExists("hamlet.txt");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM);
		
		@SuppressWarnings("unused")
		DataStream<Tuple3<String, Integer, Long>> dataStream = env
				.readTextStream("src/test/resources/testdata/hamlet.txt")
				.flatMap(new WindowWordCountSplitter())
				.partitionBy(0)
				.flatMap(new WindowWordCountCounter(10, 2, 1, 1))
				.addSink(new WindowWordCountSink());
		
		env.execute();
	}
}
