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

package org.apache.flink.graph.library;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * An implementation of the label propagation algorithm. The iterative algorithm
 * detects communities by propagating labels. In each iteration, a vertex adopts
 * the label that is most frequent among its neighbors' labels. Labels are
 * represented by Longs and we assume a total ordering among them, in order to
 * break ties. The algorithm converges when no vertex changes its value or the
 * maximum number of iterations have been reached. Note that different
 * initializations might lead to different results.
 * 
 */
@SuppressWarnings("serial")

public class LabelPropagationAlgorithm<K extends Comparable<K> & Serializable>
		implements GraphAlgorithm<K, Long, NullValue> {

	private final int maxIterations;

	public LabelPropagationAlgorithm(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<K, Long, NullValue> run(Graph<K, Long, NullValue> input) {

		// iteratively adopt the most frequent label among the neighbors
		// of each vertex
		return input.runVertexCentricIteration(new UpdateVertexLabel<K>(), new SendNewLabelToNeighbors<K>(),
				maxIterations);
	}

	/**
	 * Function that updates the value of a vertex by adopting the most frequent
	 * label among its in-neighbors
	 */
	public static final class UpdateVertexLabel<K> extends VertexUpdateFunction<K, Long, Long> {

		public void updateVertex(Vertex<K, Long> vertex,
				MessageIterator<Long> inMessages) {
			Map<Long, Long> labelsWithFrequencies = new HashMap<Long, Long>();

			long maxFrequency = 1;
			long mostFrequentLabel = vertex.getValue();

			// store the labels with their frequencies
			for (Long msg : inMessages) {
				if (labelsWithFrequencies.containsKey(msg)) {
					long currentFreq = labelsWithFrequencies.get(msg);
					labelsWithFrequencies.put(msg, currentFreq + 1);
				} else {
					labelsWithFrequencies.put(msg, 1L);
				}
			}
			// select the most frequent label: if two or more labels have the
			// same frequency,
			// the node adopts the label with the highest value
			for (Entry<Long, Long> entry : labelsWithFrequencies.entrySet()) {
				if (entry.getValue() == maxFrequency) {
					// check the label value to break ties
					if (entry.getKey() > mostFrequentLabel) {
						mostFrequentLabel = entry.getKey();
					}
				} else if (entry.getValue() > maxFrequency) {
					maxFrequency = entry.getValue();
					mostFrequentLabel = entry.getKey();
				}
			}

			// set the new vertex value
			setNewVertexValue(mostFrequentLabel);
		}
	}

	/**
	 * Sends the vertex label to all out-neighbors
	 */
	public static final class SendNewLabelToNeighbors<K> extends MessagingFunction<K, Long, Long, NullValue> {

		public void sendMessages(Vertex<K, Long> vertex) {
			sendMessageToAllNeighbors(vertex.getValue());
		}
	}
}