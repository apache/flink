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

package eu.stratosphere.streaming.examples.wordcount;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.util.Collector;

public class WordCountSplitter extends FlatMapFunction<Tuple1<String>, Tuple1<String>> {
	private static final long serialVersionUID = 1L;

	private Tuple1<String> outTuple = new Tuple1<String>();

	// Splits the lines according on spaces
	@Override
	public void flatMap(Tuple1<String> inTuple, Collector<Tuple1<String>> out) throws Exception {
		
		for (String word : inTuple.f0.split(" ")) {
			outTuple.f0 = word;
			out.collect(outTuple);
		}
	}
}