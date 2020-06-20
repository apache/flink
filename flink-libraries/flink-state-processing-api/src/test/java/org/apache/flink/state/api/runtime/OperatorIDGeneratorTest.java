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

package org.apache.flink.state.api.runtime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import org.junit.Assert;
import org.junit.Test;

import java.util.stream.StreamSupport;

/**
 * Test that {@code OperatorIDGenerator} creates ids
 * from uids exactly the same as the job graph generator.
 */
public class OperatorIDGeneratorTest {
	private static final String UID = "uid";

	private static final String OPERATOR_NAME = "operator";

	@Test
	public void testOperatorIdMatchesUid() {
		OperatorID expectedId = getOperatorID();

		OperatorID generatedId = OperatorIDGenerator.fromUid(UID);

		Assert.assertEquals(expectedId, generatedId);
	}

	private static OperatorID getOperatorID() {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env
			.fromElements(1, 2, 3)
			.uid(UID).name(OPERATOR_NAME)
			.disableChaining()
			.addSink(new DiscardingSink<>());

		JobGraph graph = env.getStreamGraph().getJobGraph(new JobID());
		JobVertex vertex = StreamSupport.stream(graph.getVertices().spliterator(), false)
			.filter(node -> node.getName().contains(OPERATOR_NAME))
			.findFirst()
			.orElseThrow(() -> new IllegalStateException("Unable to find vertex"));

		return vertex.getOperatorIDs().get(0).getGeneratedOperatorID();
	}
}
