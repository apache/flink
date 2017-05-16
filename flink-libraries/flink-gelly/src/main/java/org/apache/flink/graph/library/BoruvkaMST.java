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

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Map;

/**
 * This is an implementation of the Boruvka minimum spanning tree algorithm,
 * using vertex-centric iterations.
 * <p>
 * The input graph will be taken as undirected.
 * The picked edges are restricted to match src &lt target.
 *
 * @param <K> the id type for vertices
 * @param <V> the value type for vertices
 * @param <W> the edge weight type
 */
@SuppressWarnings("serial")
public class BoruvkaMST<K extends Comparable<K>, V, W extends Comparable<W>>
		implements GraphAlgorithm<K, V, W, DataSet<Edge<K, W>>> {

	private static final String STEP_AGGREGATOR = "STEP_AGGREGATOR";

	private int parallelism;
	private int maxIterationTimes;

	/**
	 * Creates an instance of the Boruvka MST algorithm.
	 *
	 * @param maxIterationTimes the maximum number of iterations
	 * @param parallelism       the parallelism of the iteration
	 */
	public BoruvkaMST(int maxIterationTimes, int parallelism) {
		this.parallelism = parallelism;
		this.maxIterationTimes = maxIterationTimes;
	}

	/**
	 * Creates an instance of the Boruvka MST algorithm with parallelism = 1.
	 *
	 * @param maxIterationTimes the maximum number of iterations
	 */
	public BoruvkaMST(int maxIterationTimes) {
		this(maxIterationTimes, 1);
	}

	/**
	 * Get the iteration parallelism.
	 *
	 * @return the iteration parallelism
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Set the iteration parallelism.
	 *
	 * @param parallelism the parallelism to set
	 */
	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	/**
	 * Get the maximum iteration times.
	 *
	 * @return the maximum iteration times
	 */
	public int getMaxIterationTimes() {
		return maxIterationTimes;
	}

	/**
	 * Set the maximum iteration times.
	 *
	 * @param maxIterationTimes the maximum iteration times
	 */
	public void setMaxIterationTimes(int maxIterationTimes) {
		this.maxIterationTimes = maxIterationTimes;
	}

	@Override
	public DataSet<Edge<K, W>> run(Graph<K, V, W> input) throws Exception {
		Graph<K, MSTVertexValue<K, W>, W> graph = input.mapVertices(
				new VertexValueInitializer<K, V, W>()).getUndirected();
		VertexCentricConfiguration parameters = new VertexCentricConfiguration();
		parameters.setName("Boruvka MST Iteration");
		parameters.setParallelism(parallelism);
		parameters.registerAggregator(STEP_AGGREGATOR, new LongSumAggregator());
		Graph<K, MSTVertexValue<K, W>, W> result = graph.
				runVertexCentricIteration(new MSTIteration<K, W>(),
						new MSTMessageCombiner<K, W>(),
						maxIterationTimes, parameters);
		return result.getVertices().flatMap(new FlatMapFunction<Vertex<K, MSTVertexValue<K, W>>, Edge<K, W>>() {
			@Override
			public void flatMap(Vertex<K, MSTVertexValue<K, W>> value, Collector<Edge<K, W>> out) throws Exception {
				if (value.f1.getPickedEdge() != null) {
					out.collect(value.f1.getPickedEdge());
				}
			}
		}).returns(graph.getEdges().getType());
	}

	/**
	 * A MapFunction used to initialize the graph with {@link MSTVertexValue}.
	 * Since the adjacent edges are not available here,
	 * an extra initialization will be done in the iteration.
	 *
	 * @param <K> the id type for vertices
	 * @param <V> the value type for vertices
	 * @param <W> the edge weight type
	 */
	@SuppressWarnings("serial")
	private static class VertexValueInitializer<K extends Comparable<K>, V, W extends Comparable<W>>
			implements MapFunction<Vertex<K, V>, MSTVertexValue<K, W>> {
		@Override
		public MSTVertexValue<K, W> map(Vertex<K, V> value) throws Exception {
			return new MSTVertexValue<>(value.getId());
		}
	}

	/**
	 * The iteration for Boruvka MST algorithm contains 5 different steps:
	 * <ol>
	 * <li>Initialize the vertices with adjacent edges.</li>
	 * <li>Each vertex picks an (virtually) adjacent edge with the minimum weight.</li>
	 * <li>Choose super vertices and search for linked super vertices.</li>
	 * <li>Notify (virtual) neighbours the super vertex info.</li>
	 * <li>Collapse edges that connect to other sub-trees to each super vertex.</li>
	 * </ol>
	 * The Iteration goes from step 2 to step 5.
	 *
	 * @param <K> the id type for vertices
	 * @param <W> the edge weight type
	 */
	@SuppressWarnings("serial")
	private static final class MSTIteration<K extends Comparable<K>, W extends Comparable<W>>
			extends ComputeFunction<K, MSTVertexValue<K, W>, W, MSTMessage<K, W>> {

		enum Step {
			INIT, PICK_MIN_EDGE, FIND_SUPER_VERTEX, NOTIFY_NEIGHBOURS, COLLAPSE_EDGES
		}

		private Step step;

		private MSTIteration() {
			step = null;
		}

		@Override
		public void preSuperstep() throws Exception {
			LongValue nextStep = (getPreviousIterationAggregate(STEP_AGGREGATOR));
			if (null == nextStep || nextStep.getValue() == 0L) {
				nextStep();
			}
		}

		@Override
		public void compute(Vertex<K, MSTVertexValue<K, W>> vertex, MessageIterator<MSTMessage<K, W>> mstMessages)
				throws Exception {
			MSTVertexValue<K, W> vertexValue = vertex.f1;
			boolean updated = false;
			switch (step) {
				case INIT:
					updated = initialize(vertexValue);
					break;
				case PICK_MIN_EDGE:
					updated = pickMinEdge(vertexValue);
					break;
				case FIND_SUPER_VERTEX:
					updated = findSuperVertex(vertexValue, mstMessages);
					break;
				case NOTIFY_NEIGHBOURS:
					updated = notifyNeighbours(vertexValue, mstMessages);
					break;
				case COLLAPSE_EDGES:
					updated = collapseEdges(vertexValue, mstMessages);
					break;
				default:

			}
			if (updated) {
				setNewVertexValue(vertexValue);
			}
		}

		/**
		 * To prevent the vertex from halting.
		 *
		 * @param vertexValue current vertex value
		 */
		private void sendHeartbeat(MSTVertexValue<K, W> vertexValue) {
			sendMessageTo(vertexValue.id, new MSTMessage<K, W>(MSTMessage.Type.HEARTBEAT, vertexValue.id,
					vertexValue.id));
		}

		/**
		 * Go to the next step.
		 */
		private void nextStep() {
			if (null == step) {
				step = Step.INIT;
			} else if (step.ordinal() < Step.values().length - 1) {
				step = Step.values()[step.ordinal() + 1];
			} else {
				step = Step.PICK_MIN_EDGE;
			}
		}

		/**
		 * Step-1: Initialize the vertex value with edges.
		 *
		 * @param vertexValue the current vertex value
		 * @return whether the vertex value should be update
		 */
		private boolean initialize(MSTVertexValue<K, W> vertexValue) {
			VirtualEdge<K, W> vEdge;
			for (Edge<K, W> edge : getEdges()) {
				vEdge = new VirtualEdge<>(edge);
				vertexValue.addMinVirtualEdge(vEdge);
			}
			sendHeartbeat(vertexValue);
			return true;
		}

		/**
		 * Step-2: Pick an edge with the minimum weight.
		 *
		 * @param vertexValue the current vertex value
		 * @return whether the vertex value should be update
		 */
		private boolean pickMinEdge(MSTVertexValue<K, W> vertexValue) {
			boolean updated = false;
			vertexValue.resetMinWeight();
			if (vertexValue.isValid()) {
				Map.Entry<K, VirtualEdge<K, W>> minEntry = null;
				for (Map.Entry<K, VirtualEdge<K, W>> entry : vertexValue.getVirtualEdges().entrySet()) {
					if (null == vertexValue.minEdgeWeight ||
							entry.getValue().value.compareTo(vertexValue.minEdgeWeight) < 0) {
						vertexValue.setMinEdgeWeight(entry.getValue().value);
						minEntry = entry;
					}
				}
				if (null != minEntry) {
					updated = true;
					vertexValue.makeSuperVertex(false);
					vertexValue.setLinkedSuperVertex(null);
					vertexValue.setPointer(minEntry.getKey());
					vertexValue.pickEdge(vertexValue.getVirtualEdges().get(vertexValue.pointer).genEdge());
					vertexValue.clearNeighbourSuperVertex();
					vertexValue.setNotified(false);
					vertexValue.setCollapsed(false);
					sendMessageTo(vertexValue.pointer, new MSTMessage<K, W>(
							MSTMessage.Type.ASK_FOR_SUPER_VERTEX, vertexValue.id, vertexValue.pointer));
				}
			}
			return updated;
		}

		/**
		 * Step-3: Iteratively search for the linked super vertex.
		 *
		 * @param vertexValue the current vertex value
		 * @param mstMessages the message received
		 * @return whether the vertex value should be update
		 */
		private boolean findSuperVertex(MSTVertexValue<K, W> vertexValue, MessageIterator<MSTMessage<K, W>>
				mstMessages) {
			boolean updated = false;
			if (vertexValue.isValid()) {
				for (MSTMessage<K, W> mstMessage : mstMessages) {
					switch (mstMessage.type) {
						case ASK_FOR_SUPER_VERTEX:
							if (vertexValue.pointer.compareTo(mstMessage.src) == 0
									&& vertexValue.id.compareTo(mstMessage.src) < 0 && !vertexValue.isSuperVertex()) {
								if (!vertexValue.isLinkedToSuperVertex()) {
									vertexValue.setLinkedSuperVertex(vertexValue.id);
								}
								vertexValue.makeSuperVertex(true);
								vertexValue.unpickEdge();
								updated = true;
							}
							sendMessageTo(mstMessage.src,
									new MSTMessage<K, W>(MSTMessage.Type.ANSWER_FOR_SUPER_VERTEX, vertexValue.id,
											vertexValue.getLinkedSuperVertex()));
							((LongSumAggregator) getIterationAggregator(STEP_AGGREGATOR)).aggregate(1L);
							break;
						case ANSWER_FOR_SUPER_VERTEX:
							if (!vertexValue.isLinkedToSuperVertex()) {
								vertexValue.setLinkedSuperVertex(mstMessage.value);
								updated = true;
							}
							break;
						default:
					}
				}
				if (!vertexValue.isLinkedToSuperVertex()) {
					sendMessageTo(vertexValue.pointer, new MSTMessage<K, W>(MSTMessage.Type.ASK_FOR_SUPER_VERTEX,
							vertexValue.id, vertexValue.pointer));
				}
				sendHeartbeat(vertexValue);
			}
			return updated;
		}

		/**
		 * Step-4: Notify virtual neighbours the super vertex info.
		 *
		 * @param vertexValue the current vertex value
		 * @param mstMessages the message received
		 * @return whether the vertex value should be update
		 */
		private boolean notifyNeighbours(MSTVertexValue<K, W> vertexValue, MessageIterator<MSTMessage<K, W>> mstMessages) {
			boolean updated = false;
			if (mstMessages.hasNext()) {
				updated = true;
			}
			if (!vertexValue.hasNotified()) {
				vertexValue.setNotified(true);
				for (K neighbour : vertexValue.getVirtualEdges().keySet()) {
					sendMessageTo(neighbour, new MSTMessage<K, W>(MSTMessage.Type.NOTIFY_SUPER_VERTEX, vertexValue
							.id, vertexValue.linkedSuperVertex));
				}
				((LongSumAggregator) getIterationAggregator(STEP_AGGREGATOR)).aggregate(1L);
			}
			for (MSTMessage<K, W> message : mstMessages) {
				switch (message.type) {
					case NOTIFY_SUPER_VERTEX:
						vertexValue.setNeighbourSuperVertex(message.src, message.value);
						break;
					default:
				}
			}
			sendHeartbeat(vertexValue);
			return updated;
		}

		/**
		 * Step-4: Collapse the outer linked edges to each super vertex.
		 *
		 * @param vertexValue the current vertex value
		 * @param mstMessages the message received
		 * @return whether the vertex value should be update
		 */
		private boolean collapseEdges(MSTVertexValue<K, W> vertexValue, MessageIterator<MSTMessage<K, W>> mstMessages) {
			boolean updated = false;
			K superVertexForNeighbour;
			VirtualEdge<K, W> newEdge;
			if (!vertexValue.isCollapsed()) {
				vertexValue.setCollapsed(true);
				updated = true;
				for (VirtualEdge<K, W> edge : vertexValue.getVirtualEdges().values()) {
					superVertexForNeighbour = vertexValue.getNeighbourSuperVertex(edge.vTarget);
					if (0 != superVertexForNeighbour.compareTo(vertexValue.getLinkedSuperVertex())) {
						newEdge = new VirtualEdge<>(edge.src, edge.target, superVertexForNeighbour, edge.value);
						sendMessageTo(vertexValue.getLinkedSuperVertex(),
								new MSTMessage<>(MSTMessage.Type.COLLAPSE_EDGE, vertexValue.id, newEdge));
					}
				}
				((LongSumAggregator) getIterationAggregator(STEP_AGGREGATOR)).aggregate(1L);
				if (vertexValue.isSuperVertex()) {
					sendHeartbeat(vertexValue);
				}
			}
			if (vertexValue.isSuperVertex()) {
				vertexValue.clearVirtualEdges();
				for (MSTMessage<K, W> message : mstMessages) {
					switch (message.type) {
						case COLLAPSE_EDGE:
							vertexValue.addMinVirtualEdge(message.edge);
							updated = true;
							break;
						default:
					}
				}
				sendHeartbeat(vertexValue);
			} else {
				vertexValue.setValid(false);
			}
			return updated;
		}
	}

	/**
	 * The combiner for messages with different {@link MSTMessage.Type}.
	 *
	 * @param <K> the id type for vertices
	 * @param <W> the edge weight type
	 */
	@SuppressWarnings("serial")
	private static final class MSTMessageCombiner<K extends Comparable<K>, W
			extends Comparable<W>> extends MessageCombiner<K, MSTMessage<K, W>> {

		@Override
		public void combineMessages(MessageIterator<MSTMessage<K, W>> mstMessages) throws Exception {
			if (mstMessages.hasNext()) {
				MSTMessage<K, W> message = mstMessages.next();
				//Send the heartbeats.
				while (true) {
					if (message.type == MSTMessage.Type.HEARTBEAT) {
						sendCombinedMessage(message);
					} else {
						break;
					}
					if (mstMessages.hasNext()) {
						message = mstMessages.next();
					} else {
						break;
					}
				}
				if (message.type != MSTMessage.Type.HEARTBEAT) {
					switch (message.type) {
						case ASK_FOR_SUPER_VERTEX:
						case ANSWER_FOR_SUPER_VERTEX:
							MSTMessage<K, W> answerMessage = null;
							while (true) {
								if (message.type == MSTMessage.Type.ANSWER_FOR_SUPER_VERTEX) {
									//Only reserve the one with a super vertex value.
									if (null == answerMessage || message.value != null) {
										answerMessage = message;
									}
								} else {
									sendCombinedMessage(message);
								}
								if (!mstMessages.hasNext()) {
									break;
								} else {
									message = mstMessages.next();
								}
							}
							if (null != answerMessage) {
								sendCombinedMessage(answerMessage);
							}
							break;
						case COLLAPSE_EDGE:
							Map<K, MSTMessage<K, W>> buffer = new HashMap<>();
							MSTMessage<K, W> oldMessage;
							while (true) {
								if (message.type == MSTMessage.Type.COLLAPSE_EDGE) {
									oldMessage = buffer.get(message.edge.vTarget);
									//For each virtual target, reserve one with the minimum edge weight.
									if (null == oldMessage || message.edge.value.compareTo(oldMessage.edge.value) < 0) {
										buffer.put(message.edge.vTarget, message);
									}
								} else {
									sendCombinedMessage(message);
								}
								if (mstMessages.hasNext()) {
									message = mstMessages.next();
								} else {
									break;
								}
							}
							for (MSTMessage<K, W> m : buffer.values()) {
								sendCombinedMessage(m);
							}
							break;
						default:
							while (true) {
								sendCombinedMessage(message);
								if (mstMessages.hasNext()) {
									message = mstMessages.next();
								} else {
									break;
								}
							}
					}
				}
			}
		}
	}


	/**
	 * The vertex value used for MST iteration.
	 *
	 * @param <K> the id type for vertices
	 * @param <W> the edge weight type
	 */
	private static final class MSTVertexValue<K extends Comparable<K>, W extends Comparable<W>> {

		private K id;
		private Boolean valid;
		private Boolean superVertexFlag;
		private K linkedSuperVertex;
		private K pointer;
		private W minEdgeWeight;
		private Boolean notified;
		private Boolean collapsed;
		private Edge<K, W> pickedEdge;
		private HashMap<K, VirtualEdge<K, W>> virtualEdges;
		private HashMap<K, K> neighbourSuperVertex;

		/**
		 * Initialize a vertex value with a {@link Comparable} id.
		 *
		 * @param id the id that can represent a vertex
		 */
		private MSTVertexValue(K id) {
			this.id = id;
			this.valid = true;
			this.superVertexFlag = true;
			this.linkedSuperVertex = id;
			this.minEdgeWeight = null;
			this.pointer = null;
			pickedEdge = null;
			virtualEdges = new HashMap<>();
			neighbourSuperVertex = new HashMap<>();
		}

		private boolean isCollapsed() {
			return collapsed;
		}

		private void setCollapsed(boolean collapsed) {
			this.collapsed = collapsed;
		}

		private boolean isValid() {
			return valid;
		}

		private void setValid(boolean valid) {
			this.valid = valid;
		}

		private boolean hasNotified() {
			return notified;
		}

		private void setNotified(boolean notified) {
			this.notified = notified;
		}

		private void setMinEdgeWeight(W weight) {
			this.minEdgeWeight = weight;
		}

		private void addMinVirtualEdge(VirtualEdge<K, W> edge) {
			// For each other super vertex, only reserve an edge with the minimum weight.
			if (!virtualEdges.containsKey(edge.vTarget) ||
					virtualEdges.get(edge.vTarget).value.compareTo(edge.value) > 0) {
				virtualEdges.put(edge.vTarget, edge);
			}
		}

		private void resetMinWeight() {
			minEdgeWeight = null;
		}

		private Map<K, VirtualEdge<K, W>> getVirtualEdges() {
			return virtualEdges;
		}

		private void setNeighbourSuperVertex(K neighbour, K superVertex) {
			neighbourSuperVertex.put(neighbour, superVertex);
		}

		private K getNeighbourSuperVertex(K neighbour) {
			return neighbourSuperVertex.get(neighbour);
		}

		private void clearNeighbourSuperVertex() {
			neighbourSuperVertex.clear();
		}

		private void clearVirtualEdges() {
			virtualEdges.clear();
		}

		private void pickEdge(Edge<K, W> edge) {
			pickedEdge = new Edge<>();
			// To make sure src < target for each picked edge
			if (edge.getSource().compareTo(edge.getTarget()) < 0) {
				pickedEdge.setSource(edge.getSource());
				pickedEdge.setTarget(edge.getTarget());
			} else {
				pickedEdge.setSource(edge.getTarget());
				pickedEdge.setTarget(edge.getSource());
			}
			pickedEdge.setValue(edge.getValue());
		}

		private void unpickEdge() {
			pickedEdge = null;
		}

		private void setPointer(K vertexId) {
			this.pointer = vertexId;
		}

		private boolean isSuperVertex() {
			return superVertexFlag;
		}

		private void makeSuperVertex(boolean flag) {
			this.superVertexFlag = flag;
			if (flag) {
				this.linkedSuperVertex = id;
			}
		}

		private Edge<K, W> getPickedEdge() {
			return pickedEdge;
		}

		private void setLinkedSuperVertex(K vertexId) {
			this.linkedSuperVertex = vertexId;
		}

		private K getLinkedSuperVertex() {
			return linkedSuperVertex;
		}

		private boolean isLinkedToSuperVertex() {
			return null != linkedSuperVertex;
		}
	}


	/**
	 * A VirtualEdge contains a real edge, as well as its virtual target
	 * that point to another super vertex.
	 *
	 * @param <K> the id type for vertices
	 * @param <W> the edge weight type
	 */
	private static final class VirtualEdge<K extends Comparable<K>, W extends Comparable<W>> {

		private K src;
		private K target;
		private K vTarget;
		private W value;

		private VirtualEdge(K src, K target, K vTarget, W value) {
			this.src = src;
			this.target = target;
			this.vTarget = vTarget;
			this.value = value;
		}

		private VirtualEdge(K src, K target, W value) {
			this(src, target, target, value);
		}

		private VirtualEdge(Edge<K, W> edge) {
			this(edge.getSource(), edge.getTarget(), edge.getValue());
		}

		private Edge<K, W> genEdge() {
			Edge<K, W> edge = new Edge<>();
			edge.setSource(src);
			edge.setTarget(target);
			edge.setValue(value);
			return edge;
		}
	}

	/**
	 * The message for data exchange between vertices.
	 *
	 * @param <K> the id type for vertices
	 * @param <W> the edge weight type
	 */
	private static final class MSTMessage<K extends Comparable<K>, W extends Comparable<W>> {

		enum Type {
			HEARTBEAT, ASK_FOR_SUPER_VERTEX, ANSWER_FOR_SUPER_VERTEX, NOTIFY_SUPER_VERTEX, COLLAPSE_EDGE,
		}

		private Type type;
		private K src;
		private K value;
		private VirtualEdge<K, W> edge;

		private MSTMessage(Type type, K src, K value) {
			this.src = src;
			this.type = type;
			this.value = value;
		}

		private MSTMessage(Type type, K src, VirtualEdge<K, W> edge) {
			this.type = type;
			this.src = src;
			this.edge = edge;
		}
	}
}

