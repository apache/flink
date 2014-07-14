/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package org.apache.flink.streaming.examples.window.sum;

import org.apache.flink.streaming.api.DataStream;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;

import org.apache.flink.api.java.tuple.Tuple2;

public class WindowSumLocal {
	
	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM);
		
		DataStream<Tuple2<Integer, Long>> dataStream = env
				.addSource(new WindowSumSource(), SOURCE_PARALLELISM)
				.map(new WindowSumMultiple())
				.flatMap(new WindowSumAggregate(100, 20, 10, 1))
				.addSink(new WindowSumSink());
		
		env.execute();
	}
}
