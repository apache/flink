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

package org.apache.flink.streaming.api.operators.windowing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.util.Collector;

/**
 * This FlatMapFunction is used to send the number of parts for each window ID
 * (for each parallel discretizer) to the parallel merger that will use is to
 * merge parallel discretized windows
 */
public class WindowPartExtractor<OUT> implements FlatMapFunction<StreamWindow<OUT>, Tuple2<Integer, Integer>> {

	private static final long serialVersionUID = 1L;

	Integer lastIndex = -1;

	@Override
	public void flatMap(StreamWindow<OUT> value, Collector<Tuple2<Integer, Integer>> out)
			throws Exception {

		// We dont emit new values for the same index, this avoids sending the
		// same information for the same partitioned window multiple times
		if (value.windowID != lastIndex) {
			
			// For empty windows we send 0 since these windows will be filtered
			// out
			if (value.isEmpty()) {
				out.collect(new Tuple2<Integer, Integer>(value.windowID, 0));
			} else {
				out.collect(new Tuple2<Integer, Integer>(value.windowID, value.numberOfParts));
			}
			lastIndex = value.windowID;
		}
	}

}
