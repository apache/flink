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

package org.apache.flink.streaming.examples.iterative.kmeans;

import org.apache.flink.streaming.api.DataStream;
import org.apache.flink.streaming.api.JobGraphBuilder;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;
import org.apache.flink.streaming.faulttolerance.FaultToleranceType;
import org.apache.flink.streaming.util.ClusterUtil;
import org.apache.flink.streaming.util.LogUtils;
import org.apache.log4j.Level;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.nephele.jobgraph.JobGraph;

public class KMeansLocal {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

//		@SuppressWarnings("unused")
//		DataStream<Tuple2<String, Integer>> dataStream = env
//				.addSource(new KMeansSource(2, 2, 1, 5))
//				.addFixPoint(new KMeansMap(), new KMeansReduce(), 20)
//				.addSink(new KMeansSink());
		
		env.execute();
	}
}
