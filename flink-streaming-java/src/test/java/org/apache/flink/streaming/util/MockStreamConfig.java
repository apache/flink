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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;

import java.util.ArrayList;
import java.util.List;

/**
 * A dummy stream config implementation for specifying the number of outputs in tests.
 */
public class MockStreamConfig extends StreamConfig {

	public MockStreamConfig(Configuration configuration, int numberOfOutputs) {
		super(configuration);

		setChainStart();
		setNumberOfOutputs(numberOfOutputs);
		setTypeSerializerOut(new StringSerializer());
		setVertexID(0);
		setStreamOperator(new TestSequentialReadingStreamOperator("test operator"));
		setOperatorID(new OperatorID());

		StreamOperator dummyOperator = new AbstractStreamOperator() {
			private static final long serialVersionUID = 1L;
		};

		StreamNode sourceVertex = new StreamNode(0, null, null, dummyOperator, "source", SourceStreamTask.class);
		StreamNode targetVertex = new StreamNode(1, null, null, dummyOperator, "target", SourceStreamTask.class);

		List<StreamEdge> outEdgesInOrder = new ArrayList<>(numberOfOutputs);
		for (int i = 0; i < numberOfOutputs; i++) {
			outEdgesInOrder.add(
				new StreamEdge(sourceVertex, targetVertex, numberOfOutputs, new BroadcastPartitioner<>(), null));
		}
		setOutEdgesInOrder(outEdgesInOrder);
		setNonChainedOutputs(outEdgesInOrder);
	}
}
