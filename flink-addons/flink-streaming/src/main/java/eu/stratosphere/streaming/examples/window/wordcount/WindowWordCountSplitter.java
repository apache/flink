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

package eu.stratosphere.streaming.examples.window.wordcount;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.functions.FlatMapFunction;

public class WindowWordCountSplitter extends FlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
	private static final long serialVersionUID = 1L;
	
	private String[] words = new String[] {};
	private Long timestamp = 0L;
	private Tuple2<String, Long> outTuple = new Tuple2<String, Long>();

	@Override
	public void flatMap(Tuple2<String, Long> inTuple, Collector<Tuple2<String, Long>> out) throws Exception {

		words=inTuple.f0.split(" ");
		timestamp=inTuple.f1;
		for(String word : words){
			outTuple.f0 = word;
			outTuple.f1 = timestamp;
			out.collect(outTuple);
		}
	}
}