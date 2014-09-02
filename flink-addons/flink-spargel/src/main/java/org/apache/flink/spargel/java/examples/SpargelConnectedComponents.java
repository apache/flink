/**
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

package org.apache.flink.spargel.java.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.VertexCentricIteration;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

@SuppressWarnings({"serial", "unchecked"})
public class SpargelConnectedComponents {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Long> vertexIds = env.generateSequence(0, 10);
		DataSet<Tuple2<Long, Long>> edges = env.fromElements(new Tuple2<Long, Long>(0L, 2L), new Tuple2<Long, Long>(2L, 4L), new Tuple2<Long, Long>(4L, 8L),
															new Tuple2<Long, Long>(1L, 5L), new Tuple2<Long, Long>(3L, 7L), new Tuple2<Long, Long>(3L, 9L));
		
		DataSet<Tuple2<Long, Long>> initialVertices = vertexIds.map(new IdAssigner());
		
		DataSet<Tuple2<Long, Long>> result = initialVertices.runOperation(VertexCentricIteration.withPlainEdges(edges, new CCUpdater(), new CCMessager(), 100));
		
		result.print();
		env.execute("Spargel Connected Components");
	}
	
	public static final class CCUpdater extends VertexUpdateFunction<Long, Long, Long> {
		@Override
		public void updateVertex(Long vertexKey, Long vertexValue, MessageIterator<Long> inMessages) {
			long min = Long.MAX_VALUE;
			for (long msg : inMessages) {
				min = Math.min(min, msg);
			}
			if (min < vertexValue) {
				setNewVertexValue(min);
			}
		}
	}
	
	public static final class CCMessager extends MessagingFunction<Long, Long, Long, NullValue> {
		@Override
		public void sendMessages(Long vertexId, Long componentId) {
			sendMessageToAllNeighbors(componentId);
		}
	}
	
	/**
	 * A map function that takes a Long value and creates a 2-tuple out of it:
	 * <pre>(Long value) -> (value, value)</pre>
	 */
	public static final class IdAssigner implements MapFunction<Long, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(Long value) {
			return new Tuple2<Long, Long>(value, value);
		}
	}
}
