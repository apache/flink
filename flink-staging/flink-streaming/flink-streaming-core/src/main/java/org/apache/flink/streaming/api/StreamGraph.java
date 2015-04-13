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

package org.apache.flink.streaming.api;

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
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.api.streamvertex.CoStreamVertex;
import org.apache.flink.streaming.api.streamvertex.StreamIterationHead;
import org.apache.flink.streaming.api.streamvertex.StreamIterationTail;
import org.apache.flink.streaming.api.streamvertex.StreamVertex;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
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
	private final static String DEAFULT_JOB_NAME = "Flink Streaming Job";
	private String jobName = DEAFULT_JOB_NAME;

	private final StreamExecutionEnvironment environemnt;
	private final ExecutionConfig executionConfig;

	private boolean checkpointingEnabled = false;
	private long checkpointingInterval = 5000;
	private boolean chaining = true;

	private final Map<Integer, StreamNode> streamNodes;
	private final Set<Integer> sources;

	private final Map<Integer, StreamLoop> streamLoops;
	protected final Map<Integer, StreamLoop> vertexIDtoLoop;

	public StreamGraph(StreamExecutionEnvironment environment) {

		this.environemnt = environment;
		executionConfig = environment.getConfig();

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

	public <IN, OUT> void addSource(Integer vertexID, StreamInvokable<IN, OUT> invokableObject,
			TypeInformation<IN> inTypeInfo, TypeInformation<OUT> outTypeInfo, String operatorName) {
		addOperator(vertexID, invokableObject, inTypeInfo, outTypeInfo, operatorName);
		sources.add(vertexID);
	}

	public <IN, OUT> void addOperator(Integer vertexID, StreamInvokable<IN, OUT> invokableObject,
			TypeInformation<IN> inTypeInfo, TypeInformation<OUT> outTypeInfo, String operatorName) {

		addNode(vertexID, StreamVertex.class, invokableObject, operatorName);

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
			CoInvokable<IN1, IN2, OUT> taskInvokableObject, TypeInformation<IN1> in1TypeInfo,
			TypeInformation<IN2> in2TypeInfo, TypeInformation<OUT> outTypeInfo, String operatorName) {

		addNode(vertexID, CoStreamVertex.class, taskInvokableObject, operatorName);

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

		StreamLoop iteration = new StreamLoop(iterationID, getVertex(iterationHead), timeOut);
		streamLoops.put(iterationID, iteration);
		vertexIDtoLoop.put(vertexID, iteration);

		setSerializersFrom(iterationHead, vertexID);
		getVertex(vertexID).setOperatorName("IterationHead-" + iterationHead);

		int outpartitionerIndex = getVertex(iterationHead).getInEdgeIndices().get(0);
		StreamPartitioner<?> outputPartitioner = getVertex(outpartitionerIndex).getOutEdges()
				.get(0).getPartitioner();

		addEdge(vertexID, iterationHead, outputPartitioner, 0, new ArrayList<String>());

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SOURCE: {}", vertexID);
		}

		sources.add(vertexID);
	}

	public void addIterationTail(Integer vertexID, Integer iterationTail, Integer iterationID,
			long waitTime) {

		if (getVertex(iterationTail).getBufferTimeout() == 0) {
			throw new RuntimeException("Buffer timeout 0 at iteration tail is not supported.");
		}

		addNode(vertexID, StreamIterationTail.class, null, null).setParallelism(
				getVertex(iterationTail).getParallelism());

		StreamLoop iteration = streamLoops.get(iterationID);
		iteration.setTail(getVertex(iterationTail));
		vertexIDtoLoop.put(vertexID, iteration);

		setSerializersFrom(iterationTail, vertexID);
		getVertex(vertexID).setOperatorName("IterationTail-" + iterationTail);

		setParallelism(iteration.getHead().getID(), getVertex(iterationTail).getParallelism());
		setBufferTimeout(iteration.getHead().getID(), getVertex(iterationTail).getBufferTimeout());

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SINK: {}", vertexID);
		}

	}

	protected StreamNode addNode(Integer vertexID, Class<? extends AbstractInvokable> vertexClass,
			StreamInvokable<?, ?> invokableObject, String operatorName) {

		StreamNode vertex = new StreamNode(environemnt, vertexID, invokableObject, operatorName,
				new ArrayList<OutputSelector<?>>(), vertexClass);

		streamNodes.put(vertexID, vertex);

		return vertex;
	}

	public void addEdge(Integer upStreamVertexID, Integer downStreamVertexID,
			StreamPartitioner<?> partitionerObject, int typeNumber, List<String> outputNames) {

		StreamEdge edge = new StreamEdge(getVertex(upStreamVertexID),
				getVertex(downStreamVertexID), typeNumber, outputNames, partitionerObject);
		getVertex(edge.getSourceID()).addOutEdge(edge);
		getVertex(edge.getTargetID()).addInEdge(edge);
	}

	public <T> void addOutputSelector(Integer vertexID, OutputSelector<T> outputSelector) {
		getVertex(vertexID).addOutputSelector(outputSelector);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Outputselector set for {}", vertexID);
		}

	}

	public void setParallelism(Integer vertexID, int parallelism) {
		getVertex(vertexID).setParallelism(parallelism);
	}

	public void setBufferTimeout(Integer vertexID, long bufferTimeout) {
		getVertex(vertexID).setBufferTimeout(bufferTimeout);
	}

	private void setSerializers(Integer vertexID, StreamRecordSerializer<?> in1,
			StreamRecordSerializer<?> in2, StreamRecordSerializer<?> out) {
		StreamNode vertex = getVertex(vertexID);
		vertex.setSerializerIn1(in1);
		vertex.setSerializerIn2(in2);
		vertex.setSerializerOut(out);
	}

	private void setSerializersFrom(Integer from, Integer to) {
		StreamNode fromVertex = getVertex(from);
		StreamNode toVertex = getVertex(to);

		toVertex.setSerializerIn1(fromVertex.getTypeSerializerOut());
		toVertex.setSerializerOut(fromVertex.getTypeSerializerIn1());
	}

	public <OUT> void setOutType(Integer vertexID, TypeInformation<OUT> outType) {
		StreamRecordSerializer<OUT> serializer = new StreamRecordSerializer<OUT>(outType,
				executionConfig);
		getVertex(vertexID).setSerializerOut(serializer);
	}

	public <IN, OUT> void setInvokable(Integer vertexID, StreamInvokable<IN, OUT> invokableObject) {
		getVertex(vertexID).setInvokable(invokableObject);
	}

	public void setInputFormat(Integer vertexID, InputFormat<String, ?> inputFormat) {
		getVertex(vertexID).setInputFormat(inputFormat);
	}

	public StreamNode getVertex(Integer vertexID) {
		return streamNodes.get(vertexID);
	}

	protected Collection<? extends Integer> getVertexIDs() {
		return streamNodes.keySet();
	}

	protected StreamEdge getEdge(int sourceId, int targetId) {
		Iterator<StreamEdge> outIterator = getVertex(sourceId).getOutEdges().iterator();
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

	public Set<Tuple2<Integer, StreamInvokable<?, ?>>> getInvokables() {
		Set<Tuple2<Integer, StreamInvokable<?, ?>>> invokableSet = new HashSet<Tuple2<Integer, StreamInvokable<?, ?>>>();
		for (StreamNode vertex : streamNodes.values()) {
			invokableSet.add(new Tuple2<Integer, StreamInvokable<?, ?>>(vertex.getID(), vertex
					.getInvokable()));
		}
		return invokableSet;
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
