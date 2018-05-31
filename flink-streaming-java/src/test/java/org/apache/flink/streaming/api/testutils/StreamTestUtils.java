/*
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

package org.apache.flink.streaming.api.testutils;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;

/**
 * Test utils for DataStream and KeyedStream.
 */
public class StreamTestUtils {
	public static StreamOperator<?> getOperatorForDataStream(DataStream<?> dataStream) {
		StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();
		StreamGraph streamGraph = env.getStreamGraph();
		return streamGraph.getStreamNode(dataStream.getId()).getOperator();
	}

	public static Function getFunctionForDataStream(DataStream<?> dataStream) {
		AbstractUdfStreamOperator<?, ?> operator =
				(AbstractUdfStreamOperator<?, ?>) getOperatorForDataStream(dataStream);
		return operator.getUserFunction();
	}
}
