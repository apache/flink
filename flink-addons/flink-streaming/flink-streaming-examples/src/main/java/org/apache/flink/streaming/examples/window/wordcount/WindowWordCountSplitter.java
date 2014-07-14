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

package org.apache.flink.streaming.examples.window.wordcount;

import org.apache.flink.api.java.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WindowWordCountSplitter extends FlatMapFunction<Tuple1<String>, Tuple2<String, Long>> {
	private static final long serialVersionUID = 1L;
	
	private String[] words = new String[] {};
	private Long timestamp = 0L;
	private Tuple2<String, Long> outTuple = new Tuple2<String, Long>();

	// Splits the lines according to the spaces. And adds the line's timestamp to them.
	@Override
	public void flatMap(Tuple1<String> inTuple, Collector<Tuple2<String, Long>> out) throws Exception {

		words=inTuple.f0.split(" ");
		timestamp=System.currentTimeMillis();
		for(String word : words){
			outTuple.f0 = word;
			outTuple.f1 = timestamp;
			out.collect(outTuple);
		}
	}
}