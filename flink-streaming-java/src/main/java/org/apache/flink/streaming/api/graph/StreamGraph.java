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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class representing the streaming topology. It contains all the streamNodes
 * necessary to build the jobgraph for the execution.
 *
 */
@Internal
public class StreamGraph extends StreamingPlan {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraph.class);

	private Map<Integer, StreamNode> streamNodes;
	private Set<Integer> sources;
	private Set<Integer> sinks;

	private Map<Integer, String> vertexIDtoBrokerID;
	private Map<Integer, Long> vertexIDtoLoopTimeout;
	private Set<Tuple2<StreamNode, StreamNode>> iterationSourceSinkPairs;
	private final StreamGraphProperties properties;

	public StreamGraph(StreamGraphProperties properties) {
		this.properties = properties;
		// create an empty new stream graph.
		streamNodes = new LinkedHashMap<>();
		vertexIDtoBrokerID = new LinkedHashMap<>();
		vertexIDtoLoopTimeout  = new LinkedHashMap<>();
		iterationSourceSinkPairs = new LinkedHashSet<>();
		sources = new LinkedHashSet<>();
		sinks = new LinkedHashSet<>();
	}

	public StreamGraphProperties getProperties() {
		return properties;
	}

	protected boolean isIterative() {
		return !vertexIDtoLoopTimeout.isEmpty();
	}

	/**
	 * add  a streamNode of operator to graph.
	 * @param streamNode operator node
	 */
	public void addOperator(StreamNode streamNode) {
		if (streamNodes.containsKey(streamNode.getId())) {
			throw new UnsupportedOperationException("streamNode id: " + streamNode.getId() + " has already been added.");
		}
		streamNodes.put(streamNode.getId(), streamNode);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex: {}", streamNode.getId());
		}
	}

	/**
	 * add source node to graph.
	 * @param sourceNode source node
	 */
	public void addSource(StreamNode sourceNode) {
		addOperator(sourceNode);
		sources.add(sourceNode.getId());
	}

	/**
	 * add sink node to graph.
	 * @param sinkNode sink node
	 */
	public void addSink(StreamNode sinkNode) {
		addOperator(sinkNode);
		sinks.add(sinkNode.getId());
	}

	/**
	 * add edge from upStreamVertex to downStreamVertex.
	 * @param upStreamVertexID id of upStreamVertex
	 * @param downStreamVertexID id of downSteamVertex
	 * @param inputOrder  order number of the upStreamVertex's inputGate
	 * @param partitioner for upStreamVertex to partition data to downStreamVertex
	 * @param outputNames for downStreamVertex to consumer partitial results of upStreamVertex
	 * @param outputTag tagging side output for upStreamVertex
	 */
	public void addEdge(Integer upStreamVertexID,
			Integer downStreamVertexID,
			StreamEdge.InputOrder inputOrder,
			StreamPartitioner<?> partitioner,
			List<String> outputNames,
			OutputTag outputTag) {

		StreamNode upstreamNode = getStreamNode(upStreamVertexID);
		StreamNode downstreamNode = getStreamNode(downStreamVertexID);

		// If no partitioner was specified and the parallelism of upstream and downstream
		// operator matches use forward partitioning, use rebalance otherwise.
		if (partitioner == null && upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
			partitioner = new ForwardPartitioner<Object>();
		} else if (partitioner == null) {
			partitioner = new RebalancePartitioner<Object>();
		}

		if (partitioner instanceof ForwardPartitioner) {
			if (upstreamNode.getParallelism() != downstreamNode.getParallelism()) {
				throw new UnsupportedOperationException("Forward partitioning does not allow " +
						"change of parallelism. Upstream operation: " + upstreamNode + " parallelism: " + upstreamNode.getParallelism() +
						", downstream operation: " + downstreamNode + " parallelism: " + downstreamNode.getParallelism() +
						" You must use another partitioning strategy, such as broadcast, rebalance, shuffle or global.");
			}
		}

		StreamEdge edge = new StreamEdge(upstreamNode, downstreamNode, inputOrder, outputNames, partitioner, outputTag);
		getStreamNode(edge.getSourceId()).addOutEdge(edge);
		getStreamNode(edge.getTargetId()).addInEdge(edge);
	}

	public StreamNode getStreamNode(Integer vertexID) {
		return streamNodes.get(vertexID);
	}

	protected Collection<? extends Integer> getVertexIDs() {
		return streamNodes.keySet();
	}

	public Collection<Integer> getSourceIDs() {
		return sources;
	}

	protected String getBrokerID(Integer vertexID) {
		return vertexIDtoBrokerID.get(vertexID);
	}

	protected Long getLoopTimeout(Integer vertexID) {
		return vertexIDtoLoopTimeout.get(vertexID);
	}

	public void addIterationSourceAndSink(StreamNode source, StreamNode sink, int loopId, long timeout) {
		addSource(source);
		addSink(sink);

		iterationSourceSinkPairs.add(new Tuple2<>(source, sink));
		this.vertexIDtoBrokerID.put(source.getId(), "broker-" + loopId);
		this.vertexIDtoBrokerID.put(sink.getId(), "broker-" + loopId);
		this.vertexIDtoLoopTimeout.put(source.getId(), timeout);
		this.vertexIDtoLoopTimeout.put(sink.getId(), timeout);
	}

	public Collection<StreamNode> getStreamNodes() {
		return streamNodes.values();
	}

	/**
	 * Gets the assembled {@link JobGraph}.
	 */
	@SuppressWarnings("deprecation")
	public JobGraph getJobGraph() {
		// temporarily forbid checkpointing for iterative jobs
		if (isIterative() && properties.getCheckpointConfig().isCheckpointingEnabled() && !properties.getCheckpointConfig().isForceCheckpointing()) {
			throw new UnsupportedOperationException(
					"Checkpointing is currently not supported by default for iterative jobs, as we cannot guarantee exactly once semantics. "
							+ "State checkpoints happen normally, but records in-transit during the snapshot will be lost upon failure. "
							+ "\nThe user can force enable state checkpoints with the reduced guarantees by calling: env.enableCheckpointing(interval,true)");
		}

		return StreamingJobGraphGenerator.createJobGraph(this);
	}

	@Override
	public String getStreamingPlanAsJSON() {
		try {
			return new JSONGenerator(this).getJSON();
		}
		catch (Exception e) {
			throw new RuntimeException("JSON plan creation failed", e);
		}
	}

	@Override
	public void dumpStreamingPlanAsJSON(File file) throws IOException {
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new FileOutputStream(file), false);
			pw.write(getStreamingPlanAsJSON());
			pw.flush();

		} finally {
			if (pw != null) {
				pw.close();
			}
		}
	}

	@VisibleForTesting
	public List<StreamEdge> getStreamEdges(int sourceId, int targetId) {
		List<StreamEdge> result = new ArrayList<>();
		for (StreamEdge edge : getStreamNode(sourceId).getOutEdges()) {
			if (edge.getTargetId() == targetId) {
				result.add(edge);
			}
		}

		if (result.isEmpty()) {
			throw new RuntimeException("No such edge in stream graph: " + sourceId + " -> " + targetId);
		}
		return result;
	}

	@VisibleForTesting
	public Collection<Integer> getSinkIDs() {
		return sinks;
	}

	@VisibleForTesting
	public Set<Tuple2<StreamNode, StreamNode>> getIterationSourceSinkPairs() {
		return iterationSourceSinkPairs;
	}

	@VisibleForTesting
	public Set<Tuple2<Integer, StreamOperator<?>>> getOperators() {
		Set<Tuple2<Integer, StreamOperator<?>>> operatorSet = new HashSet<>();
		for (StreamNode vertex : streamNodes.values()) {
			operatorSet.add(new Tuple2<Integer, StreamOperator<?>>(vertex.getId(), vertex
					.getOperator()));
		}
		return operatorSet;
	}
}
