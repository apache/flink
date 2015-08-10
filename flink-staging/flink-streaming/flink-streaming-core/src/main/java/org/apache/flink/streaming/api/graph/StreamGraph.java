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
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.sling.commons.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class representing the streaming topology. It contains all the information
 * necessary to build the jobgraph for the execution.
 * 
 */
public class StreamGraph extends StreamingPlan {

	/** The default interval for checkpoints, in milliseconds */
	public static final int DEFAULT_CHECKPOINTING_INTERVAL_MS = 5000;
	
	private static final Logger LOG = LoggerFactory.getLogger(StreamGraph.class);

	private String jobName = StreamExecutionEnvironment.DEFAULT_JOB_NAME;

	private final StreamExecutionEnvironment environemnt;
	private final ExecutionConfig executionConfig;

	private CheckpointingMode checkpointingMode;
	private boolean checkpointingEnabled = false;
	private long checkpointingInterval = DEFAULT_CHECKPOINTING_INTERVAL_MS;
	private boolean chaining = true;

	private Map<Integer, StreamNode> streamNodes;
	private Set<Integer> sources;

	private Map<Integer, StreamLoop> streamLoops;
	protected Map<Integer, StreamLoop> vertexIDtoLoop;
	protected Map<Integer, String> vertexIDtoBrokerID;
	private StateHandleProvider<?> stateHandleProvider;
	private boolean forceCheckpoint = false;

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
		vertexIDtoLoop = new HashMap<Integer, StreamLoop>();
		vertexIDtoBrokerID = new HashMap<Integer, String>();
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

	public void forceCheckpoint() {
		this.forceCheckpoint = true;
	}

	public void setStateHandleProvider(StateHandleProvider<?> provider) {
		this.stateHandleProvider = provider;
	}

	public StateHandleProvider<?> getStateHandleProvider() {
		return this.stateHandleProvider;
	}

	public long getCheckpointingInterval() {
		return checkpointingInterval;
	}

	// Checkpointing
	
	public boolean isChainingEnabled() {
		return chaining;
	}

	public boolean isCheckpointingEnabled() {
		return checkpointingEnabled;
	}

	public CheckpointingMode getCheckpointingMode() {
		return checkpointingMode;
	}

	public void setCheckpointingMode(CheckpointingMode checkpointingMode) {
		this.checkpointingMode = checkpointingMode;
	}
	

	public boolean isIterative() {
		return !streamLoops.isEmpty();
	}

	public <IN, OUT> void addSource(Integer vertexID, StreamOperator<OUT> operatorObject,
			TypeInformation<IN> inTypeInfo, TypeInformation<OUT> outTypeInfo, String operatorName) {
		addOperator(vertexID, operatorObject, inTypeInfo, outTypeInfo, operatorName);
		sources.add(vertexID);
	}

	public <IN, OUT> void addOperator(Integer vertexID, StreamOperator<OUT> operatorObject,
			TypeInformation<IN> inTypeInfo, TypeInformation<OUT> outTypeInfo, String operatorName) {

		if (operatorObject instanceof StreamSource) {
			addNode(vertexID, SourceStreamTask.class, operatorObject, operatorName);
		} else {
			addNode(vertexID, OneInputStreamTask.class, operatorObject, operatorName);
		}

		TypeSerializer<IN> inSerializer = inTypeInfo != null && !(inTypeInfo instanceof MissingTypeInfo) ? inTypeInfo.createSerializer(executionConfig) : null;

		TypeSerializer<OUT> outSerializer = outTypeInfo != null && !(outTypeInfo instanceof MissingTypeInfo) ? outTypeInfo.createSerializer(executionConfig) : null;

		setSerializers(vertexID, inSerializer, null, outSerializer);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex: {}", vertexID);
		}
	}

	public <IN1, IN2, OUT> void addCoOperator(Integer vertexID,
			TwoInputStreamOperator<IN1, IN2, OUT> taskoperatorObject, TypeInformation<IN1> in1TypeInfo,
			TypeInformation<IN2> in2TypeInfo, TypeInformation<OUT> outTypeInfo, String operatorName) {

		addNode(vertexID, TwoInputStreamTask.class, taskoperatorObject, operatorName);

		TypeSerializer<OUT> outSerializer = (outTypeInfo != null) && !(outTypeInfo instanceof MissingTypeInfo) ?
				outTypeInfo.createSerializer(executionConfig) : null;

		setSerializers(vertexID, in1TypeInfo.createSerializer(executionConfig), in2TypeInfo.createSerializer(executionConfig), outSerializer);

		if (LOG.isDebugEnabled()) {
			LOG.debug("CO-TASK: {}", vertexID);
		}
	}

	public void addIterationHead(Integer iterationHead, Integer iterationID, long timeOut,
			TypeInformation<?> feedbackType) {
		// If there is no loop object created for this iteration create one
		StreamLoop loop = streamLoops.get(iterationID);
		if (loop == null) {
			loop = new StreamLoop(iterationID, timeOut, feedbackType);
			streamLoops.put(iterationID, loop);
		}

		loop.addHeadOperator(getStreamNode(iterationHead));
	}

	public void addIterationTail(List<DataStream<?>> feedbackStreams, Integer iterationID,
			boolean keepPartitioning) {

		if (!streamLoops.containsKey(iterationID)) {
			throw new RuntimeException("Cannot close iteration without head operator.");
		}

		StreamLoop loop = streamLoops.get(iterationID);

		for (DataStream<?> stream : feedbackStreams) {
			loop.addTailOperator(getStreamNode(stream.getId()), stream.getPartitioner(),
					stream.getSelectedNames());
		}

		if (keepPartitioning) {
			loop.applyTailPartitioning();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void finalizeLoops() {
		
		// We create each loop separately, the order does not matter as sinks
		// and sources don't interact
		for (StreamLoop loop : streamLoops.values()) {

			// We make sure not to re-create the loops if the method is called
			// multiple times
			if (loop.getSourceSinkPairs().isEmpty()) {

				List<StreamNode> headOps = loop.getHeads();
				List<StreamNode> tailOps = loop.getTails();

				// This means that the iteration was not closed. It should not
				// be
				// allowed.
				if (tailOps.isEmpty()) {
					throw new RuntimeException("Cannot execute job with empty iterations.");
				}

				// Check whether we keep the feedback partitioning
				if (loop.keepsPartitioning()) {
					// This is the complicated case as we need to enforce
					// partitioning on the tail -> sink side, which
					// requires strict forward connections at source -> head

					// We need one source/sink pair per different head
					// parallelism
					// as we depend on strict forwards connections
					Map<Integer, List<StreamNode>> parallelismToHeads = new HashMap<Integer, List<StreamNode>>();

					// Group head operators by parallelism
					for (StreamNode head : headOps) {
						int p = head.getParallelism();
						if (!parallelismToHeads.containsKey(p)) {
							parallelismToHeads.put(p, new ArrayList<StreamNode>());
						}
						parallelismToHeads.get(p).add(head);
					}

					// We create the sink/source pair for each parallelism
					// group,
					// tails will forward to all sinks but each head operator
					// will
					// only receive from one source (corresponding to its
					// parallelism)
					int c = 0;
					for (Entry<Integer, List<StreamNode>> headGroup : parallelismToHeads.entrySet()) {
						List<StreamNode> headOpsInGroup = headGroup.getValue();

						Tuple2<StreamNode, StreamNode> sourceSinkPair = createItSourceAndSink(loop,
								c);
						StreamNode source = sourceSinkPair.f0;
						StreamNode sink = sourceSinkPair.f1;

						// We connect the source to the heads in this group
						// (forward), setting
						// type to 2 in case we have a coIteration (this sets
						// the
						// input as the second input of the co-operator)
						for (StreamNode head : headOpsInGroup) {
							int inputType = loop.isCoIteration() ? 2 : 0;
							addEdge(source.getId(), head.getId(), new RebalancePartitioner(true),
									inputType, new ArrayList<String>());
						}

						// We connect all the tails to the sink keeping the
						// partitioner
						for (int i = 0; i < tailOps.size(); i++) {
							StreamNode tail = tailOps.get(i);
							StreamPartitioner<?> partitioner = loop.getTailPartitioners().get(i);
							addEdge(tail.getId(), sink.getId(), partitioner.copy(), 0, loop
									.getTailSelectedNames().get(i));
						}

						// We set the sink/source parallelism to the group
						// parallelism
						source.setParallelism(headGroup.getKey());
						sink.setParallelism(source.getParallelism());

						// We set the proper serializers for the sink/source
						setSerializersFrom(tailOps.get(0).getId(), sink.getId());
						if (loop.isCoIteration()) {
							source.setSerializerOut(loop.getFeedbackType().createSerializer(executionConfig));
						} else {
							setSerializersFrom(headOpsInGroup.get(0).getId(), source.getId());
						}

						c++;
					}

				} else {
					// This is the most simple case, we add one iteration
					// sink/source pair with the parallelism of the first tail
					// operator. Tail operators will forward the records and
					// partitioning will be enforced from source -> head

					Tuple2<StreamNode, StreamNode> sourceSinkPair = createItSourceAndSink(loop, 0);
					StreamNode source = sourceSinkPair.f0;
					StreamNode sink = sourceSinkPair.f1;

					// We get the feedback partitioner from the first input of
					// the
					// first head.
					StreamPartitioner<?> partitioner = headOps.get(0).getInEdges().get(0)
							.getPartitioner();

					// Connect the sources to heads using this partitioner
					for (StreamNode head : headOps) {
						addEdge(source.getId(), head.getId(), partitioner.copy(), 0,
								new ArrayList<String>());
					}

					// The tails are connected to the sink with forward
					// partitioning
					for (int i = 0; i < tailOps.size(); i++) {
						StreamNode tail = tailOps.get(i);
						addEdge(tail.getId(), sink.getId(), new RebalancePartitioner(true), 0, loop
								.getTailSelectedNames().get(i));
					}

					// We set the parallelism to match the first tail op to make
					// the
					// forward more efficient
					sink.setParallelism(tailOps.get(0).getParallelism());
					source.setParallelism(sink.getParallelism());

					// We set the proper serializers
					setSerializersFrom(headOps.get(0).getId(), source.getId());
					setSerializersFrom(tailOps.get(0).getId(), sink.getId());
				}

			}

		}

	}

	private Tuple2<StreamNode, StreamNode> createItSourceAndSink(StreamLoop loop, int c) {
		StreamNode source = addNode(-1 * streamNodes.size(), StreamIterationHead.class, null, null);
		sources.add(source.getId());

		StreamNode sink = addNode(-1 * streamNodes.size(), StreamIterationTail.class, null, null);

		source.setOperatorName("IterationSource-" + loop.getID() + "_" + c);
		sink.setOperatorName("IterationSink-" + loop.getID() + "_" + c);
		vertexIDtoBrokerID.put(source.getId(), loop.getID() + "_" + c);
		vertexIDtoBrokerID.put(sink.getId(), loop.getID() + "_" + c);
		vertexIDtoLoop.put(source.getId(), loop);
		vertexIDtoLoop.put(sink.getId(), loop);
		loop.addSourceSinkPair(source, sink);

		return new Tuple2<StreamNode, StreamNode>(source, sink);
	}

	protected StreamNode addNode(Integer vertexID, Class<? extends AbstractInvokable> vertexClass,
			StreamOperator<?> operatorObject, String operatorName) {

		StreamNode vertex = new StreamNode(environemnt, vertexID, operatorObject, operatorName,
				new ArrayList<OutputSelector<?>>(), vertexClass);

		streamNodes.put(vertexID, vertex);

		return vertex;
	}

	public void addEdge(Integer upStreamVertexID, Integer downStreamVertexID,
			StreamPartitioner<?> partitionerObject, int typeNumber, List<String> outputNames) {

		StreamEdge edge = new StreamEdge(getStreamNode(upStreamVertexID),
				getStreamNode(downStreamVertexID), typeNumber, outputNames, partitionerObject);
		getStreamNode(edge.getSourceId()).addOutEdge(edge);
		getStreamNode(edge.getTargetId()).addInEdge(edge);
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

	public void setKey(Integer vertexID, KeySelector<?, ?> key) {
		getStreamNode(vertexID).setStatePartitioner(key);
	}

	public void setBufferTimeout(Integer vertexID, long bufferTimeout) {
		getStreamNode(vertexID).setBufferTimeout(bufferTimeout);
	}

	private void setSerializers(Integer vertexID, TypeSerializer<?> in1, TypeSerializer<?> in2, TypeSerializer<?> out) {
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
		getStreamNode(vertexID).setSerializerOut(outType.createSerializer(executionConfig));
	}

	public <IN, OUT> void setOperator(Integer vertexID, StreamOperator<OUT> operatorObject) {
		getStreamNode(vertexID).setOperator(operatorObject);
	}

	public void setInputFormat(Integer vertexID, InputFormat<?, ?> inputFormat) {
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

	public StreamEdge getStreamEdge(int sourceId, int targetId) {
		Iterator<StreamEdge> outIterator = getStreamNode(sourceId).getOutEdges().iterator();
		while (outIterator.hasNext()) {
			StreamEdge edge = outIterator.next();

			if (edge.getTargetId() == targetId) {
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

	public Set<Tuple2<Integer, StreamOperator<?>>> getOperators() {
		Set<Tuple2<Integer, StreamOperator<?>>> operatorSet = new HashSet<Tuple2<Integer, StreamOperator<?>>>();
		for (StreamNode vertex : streamNodes.values()) {
			operatorSet.add(new Tuple2<Integer, StreamOperator<?>>(vertex.getId(), vertex
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

	public String getBrokerID(Integer vertexID) {
		return vertexIDtoBrokerID.get(vertexID);
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
		streamNodes.remove(toRemove.getId());
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
		finalizeLoops();
		// temporarily forbid checkpointing for iterative jobs
		if (isIterative() && isCheckpointingEnabled() && !forceCheckpoint) {
			throw new UnsupportedOperationException(
					"Checkpointing is currently not supported by default for iterative jobs, as we cannot guarantee exactly once semantics. "
							+ "State checkpoints happen normally, but records in-transit during the snapshot will be lost upon failure. "
							+ "\nThe user can force enable state checkpoints with the reduced guarantees by calling: env.enableCheckpointing(interval,true)");
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

}
