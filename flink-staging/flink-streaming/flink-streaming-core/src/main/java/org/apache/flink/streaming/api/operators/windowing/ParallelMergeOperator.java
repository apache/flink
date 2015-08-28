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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.CoStreamFlatMap;
import org.apache.flink.streaming.api.windowing.StreamWindow;

public class ParallelMergeOperator<OUT> extends CoStreamFlatMap<StreamWindow<OUT>, Tuple2<Integer, Integer>, StreamWindow<OUT>> {

	private ParallelMerge<OUT> parallelMerge;

	public ParallelMergeOperator(ParallelMerge<OUT> parallelMerge) {
		super(parallelMerge);
		this.parallelMerge = parallelMerge;
	}

	@Override
	public void close() throws Exception {
		// emit remaining (partial) windows

		for (Tuple2<StreamWindow<OUT>, Integer> receivedWindow : parallelMerge.getReceivedWindows().values()) {
			getCollector().collect(receivedWindow.f0);
		}

		super.close();
	}
}
