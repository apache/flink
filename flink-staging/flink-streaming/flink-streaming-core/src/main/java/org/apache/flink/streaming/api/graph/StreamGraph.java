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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.co.CoStreamOperator;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.CoStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.sling.commons.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class representing the streaming topology. It contains all the information
 * necessary to build the jobgraph for the execution.
 * 
 */
public class StreamGraph extends StreamingPlan {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraph.class);
	private String jobName = StreamExecutionEnvironment.DEFAULT_JOB_NAME;

	private final StreamExecutionEnvironment environemnt;
	private final ExecutionConfig executionConfig;

	private boolean checkpointingEnabled = false;
	private long checkpointingInterval = 5000;
	private boolean chaining = true;

	private Map<Integer, StreamNode> streamNodes;
	private Set<Integer> sources;

	private Map<Integer, StreamLoop> streamLoops;
	protected Map<Integer, StreamLoop> vertexIDtoLoop;

	public StreamGraph(StreamExecutionEnvironment environment) {

		this.environemnt = environment;
		executionConfig = environment.getConfig();

		// create an empty new stream graph.
		clear();
	}

	/**
	 * Remove all registered nodes etc.
	 */
	public void clear() {
		streamNodes = new HashMap<Integer, StreamNode>();
		streamLoops = new HashMap<Integer, StreamLoop>();
		vertexIDtoLoop = new HashMap<Integer, StreamGraph.StreamLoop>();
		sources = new HashSet<Integer>();
	}

	protected ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void setChaining(boolean chaining) {
		this.chaining = chaining;
	}

	public void setCheckpointingEnabled(boolean checkpointingEnabled) {
		this.checkpointingEnabled = checkpointingEnabled;
	}

	public void setCheckpointingInterval(long checkpointingInterval) {
		this.checkpointingInterval = checkpointingInterval;
	}

	public long getCheckpointingInterval() {
		return checkpointingInterval;
	}

	public boolean isChainingEnabled() {
		return chaining;
	}

	public boolean isCheckpointingEnabled() {
		return checkpointingEnabled;
	}

	public boolean isIterative() {
		return !streamLoops.isEmpty();
	}

	public <IN, OUT> void addSource(Integer vertexID, StreamOperator<IN, OUT> operatorObject,
			TypeInformation<IN> inTypeInfo, TypeInformation<OUT> outTypeInfo, String operatorName) {
		addOperator(vertexID, operatorObject, inTypeInfo, outTypeInfo, operatorName);
		sources.add(vertexID);
	}

	public <IN, OUT> void addOperator(Integer vertexID, StreamOperator<IN, OUT> operatorObject,
			TypeInformation<IN> inTypeInfo, TypeInformation<OUT> outTypeInfo, String operatorName) {

		addNode(vertexID, StreamTask.class, operatorObject, operatorName);

		StreamRecordSerializer<IN> inSerializer = inTypeInfo != null ? new StreamRecordSerializer<IN>(
				inTypeInfo, executionConfig) : null;

		StreamRecordSerializer<OUT> outSerializer = (outTypeInfo != null)
				&& !(outTypeInfo instanceof MissingTypeInfo) ? new StreamRecordSerializer<OUT>(
				outTypeInfo, executionConfig) : null;

		setSerializers(vertexID, inSerializer, null, outSerializer);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex: {}", vertexID);
		}
	}

	public <IN1, IN2, OUT> void addCoOperator(Integer vertexID,
			CoStreamOperator<IN1, IN2, OUT> taskoperatorObject, TypeInformation<IN1> in1TypeInfo,
			TypeInformation<IN2> in2TypeInfo, TypeInformation<OUT> outTypeInfo, String operatorName) {

		addNode(vertexID, CoStreamTask.class, taskoperatorObject, operatorName);

		StreamRecordSerializer<OUT> outSerializer = (outTypeInfo != null)
				&& !(outTypeInfo instanceof MissingTypeInfo) ? new StreamRecordSerializer<OUT>(
				outTypeInfo, executionConfig) : null;

		setSerializers(vertexID, new StreamRecordSerializer<IN1>(in1TypeInfo, executionConfig),
				new StreamRecordSerializer<IN2>(in2TypeInfo, executionConfig), outSerializer);

		if (LOG.isDebugEnabled()) {
			LOG.debug("CO-TASK: {}", vertexID);
		}
	}

	public void addIterationHead(Integer vertexID, Integer iterationHead, Integer iterationID,
			long timeOut) {

		addNode(vertexID, StreamIterationHead.class, null, null);

		chaining = false;

		StreamLoop iteration = new StreamLoop(iterationID, getStreamNode(iterationHead), timeOut);
		streamLoops.put(iterationID, iteration);
		vertexIDtoLoop.put(vertexID, iteration);

		setSerializersFrom(iterationHead, vertexID);
		getStreamNode(vertexID).setOperatorName("IterationHead-" + iterationHead);

		int outpartitionerIndex = getStreamNode(iterationHead).getInEdgeIndices().get(0);
		StreamPartitioner<?> outputPartitioner = getStreamNode(outpartitionerIndex).getOutEdges()
				.get(0).getPartitioner();

		addEdge(vertexID, iterationHead, outputPartitioner, 0, new ArrayList<String>());

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SOURCE: {}", vertexID);
		}

		sources.add(vertexID);
	}

	public void addIterationTail(Integer vertexID, Integer iterationTail, Integer iterationID,
			long waitTime) {

		if (getStreamNode(iterationTail).getBufferTimeout() == 0) {
			throw new RuntimeException("Buffer timeout 0 at iteration tail is not supported.");
		}

		addNode(vertexID, StreamIterationTail.class, null, null).setParallelism(
				getStreamNode(iterationTail).getParallelism());

		StreamLoop iteration = streamLoops.get(iterationID);
		iteration.setTail(getStreamNode(iterationTail));
		vertexIDtoLoop.put(vertexID, iteration);

		setSerializersFrom(iterationTail, vertexID);
		getStreamNode(vertexID).setOperatorName("IterationTail-" + iterationTail);

		setParallelism(iteration.getHead().getID(), getStreamNode(iterationTail).getParallelism());
		setBufferTimeout(iteration.getHead().getID(), getStreamNode(iterationTail)
				.getBufferTimeout());

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SINK: {}", vertexID);
		}

	}

	protected StreamNode addNode(Integer vertexID, Class<? extends AbstractInvokable> vertexClass,
			StreamOperator<?, ?> operatorObject, String operatorName) {

		StreamNode vertex = new StreamNode(environemnt, vertexID, operatorObject, operatorName,
				new ArrayList<OutputSelector<?>>(), vertexClass);

		streamNodes.put(vertexID, vertex);

		return vertex;
	}

	public void addEdge(Integer upStreamVertexID, Integer downStreamVertexID,
			StreamPartitioner<?> partitionerObject, int typeNumber, List<String> outputNames) {

		StreamEdge edge = new StreamEdge(getStreamNode(upStreamVertexID),
				getStreamNode(downStreamVertexID), typeNumber, outputNames, partitionerObject);
		getStreamNode(edge.getSourceID()).addOutEdge(edge);
		getStreamNode(edge.getTargetID()).addInEdge(edge);
	}

	public <T> void addOutputSelector(Integer vertexID, OutputSelector<T> outputSelector) {
		getStreamNode(vertexID).addOutputSelector(outputSelector);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Outputselector set for {}", vertexID);
		}

	}

	public void setParallelism(Integer vertexID, int parallelism) {
		getStreamNode(vertexID).setParallelism(parallelism);
	}

	public void setBufferTimeout(Integer vertexID, long bufferTimeout) {
		getStreamNode(vertexID).setBufferTimeout(bufferTimeout);
	}

	private void setSerializers(Integer vertexID, StreamRecordSerializer<?> in1,
			StreamRecordSerializer<?> in2, StreamRecordSerializer<?> out) {
		StreamNode vertex = getStreamNode(vertexID);
		vertex.setSerializerIn1(in1);
		vertex.setSerializerIn2(in2);
		vertex.setSerializerOut(out);
	}

	private void setSerializersFrom(Integer from, Integer to) {
		StreamNode fromVertex = getStreamNode(from);
		StreamNode toVertex = getStreamNode(to);

		toVertex.setSerializerIn1(fromVertex.getTypeSerializerOut());
		toVertex.setSerializerOut(fromVertex.getTypeSerializerIn1());
	}

	public <OUT> void setOutType(Integer vertexID, TypeInformation<OUT> outType) {
		StreamRecordSerializer<OUT> serializer = new StreamRecordSerializer<OUT>(outType,
				executionConfig);
		getStreamNode(vertexID).setSerializerOut(serializer);
	}

	public <IN, OUT> void setOperator(Integer vertexID, StreamOperator<IN, OUT> operatorObject) {
		getStreamNode(vertexID).setOperator(operatorObject);
	}

	public void setInputFormat(Integer vertexID, InputFormat<String, ?> inputFormat) {
		getStreamNode(vertexID).setInputFormat(inputFormat);
	}

	public void setResourceStrategy(Integer vertexID, ResourceStrategy strategy) {
		StreamNode node = getStreamNode(vertexID);
		switch (strategy) {
		case ISOLATE:
			node.isolateSlot();
			break;
		case NEWGROUP:
			node.startNewSlotSharingGroup();
			break;
		default:
			throw new IllegalArgumentException("Unknown resource strategy");
		}
	}

	public StreamNode getStreamNode(Integer vertexID) {
		return streamNodes.get(vertexID);
	}

	protected Collection<? extends Integer> getVertexIDs() {
		return streamNodes.keySet();
	}

	protected StreamEdge getEdge(int sourceId, int targetId) {
		Iterator<StreamEdge> outIterator = getStreamNode(sourceId).getOutEdges().iterator();
		while (outIterator.hasNext()) {
			StreamEdge edge = outIterator.next();

			if (edge.getTargetID() == targetId) {
				return edge;
			}
		}

		throw new RuntimeException("No such edge in stream graph: " + sourceId + " -> " + targetId);
	}

	public Collection<Integer> getSourceIDs() {
		return sources;
	}

	public Collection<StreamNode> getStreamNodes() {
		return streamNodes.values();
	}

	public Set<Tuple2<Integer, StreamOperator<?, ?>>> getOperators() {
		Set<Tuple2<Integer, StreamOperator<?, ?>>> operatorSet = new HashSet<Tuple2<Integer, StreamOperator<?, ?>>>();
		for (StreamNode vertex : streamNodes.values()) {
			operatorSet.add(new Tuple2<Integer, StreamOperator<?, ?>>(vertex.getID(), vertex
					.getOperator()));
		}
		return operatorSet;
	}

	public Collection<StreamLoop> getStreamLoops() {
		return streamLoops.values();
	}

	public Integer getLoopID(Integer vertexID) {
		return vertexIDtoLoop.get(vertexID).getID();
	}

	public long getLoopTimeout(Integer vertexID) {
		return vertexIDtoLoop.get(vertexID).getTimeout();
	}

	protected void removeEdge(StreamEdge edge) {

		edge.getSourceVertex().getOutEdges().remove(edge);
		edge.getTargetVertex().getInEdges().remove(edge);

	}

	protected void removeVertex(StreamNode toRemove) {

		Set<StreamEdge> edgesToRemove = new HashSet<StreamEdge>();

		edgesToRemove.addAll(toRemove.getInEdges());
		edgesToRemove.addAll(toRemove.getOutEdges());

		for (StreamEdge edge : edgesToRemove) {
			removeEdge(edge);
		}
		streamNodes.remove(toRemove.getID());
	}

	/**
	 * Gets the assembled {@link JobGraph} and adds a default name for it.
	 */
	public JobGraph getJobGraph() {
		return getJobGraph(jobName);
	}

	/**
	 * Gets the assembled {@link JobGraph} and adds a user specified name for
	 * it.
	 * 
	 * @param jobGraphName
	 *            name of the jobGraph
	 */
	public JobGraph getJobGraph(String jobGraphName) {

		// temporarily forbid checkpointing for iterative jobs
		if (isIterative() && isCheckpointingEnabled()) {
			throw new UnsupportedOperationException(
					"Checkpointing is currently not supported for iterative jobs!");
		}

		setJobName(jobGraphName);

		WindowingOptimizer.optimizeGraph(this);

		StreamingJobGraphGenerator jobgraphGenerator = new StreamingJobGraphGenerator(this);

		return jobgraphGenerator.createJobGraph(jobGraphName);
	}

	@Override
	public String getStreamingPlanAsJSON() {

		WindowingOptimizer.optimizeGraph(this);

		try {
			return new JSONGenerator(this).getJSON();
		} catch (JSONException e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("JSON plan creation failed: {}", e);
			}
			return "";
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

	public static enum ResourceStrategy {
		DEFAULT, ISOLATE, NEWGROUP
	}

	/**
	 * Object for representing loops in streaming programs.
	 * 
	 */
	protected static class StreamLoop {

		private Integer loopID;

		private StreamNode head;
		private StreamNode tail;

		private Long timeout;

		public StreamLoop(Integer loopID, StreamNode head, Long timeout) {
			this.loopID = loopID;
			this.head = head;
			this.timeout = timeout;
		}

		public Integer getID() {
			return loopID;
		}

		public Long getTimeout() {
			return timeout;
		}

		public void setTail(StreamNode tail) {
			this.tail = tail;
		}

		public StreamNode getHead() {
			return head;
		}

		public StreamNode getTail() {
			return tail;
		}

	}

}
