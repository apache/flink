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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.compiler.plan.StreamingPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.api.streamvertex.CoStreamVertex;
import org.apache.flink.streaming.api.streamvertex.StreamIterationHead;
import org.apache.flink.streaming.api.streamvertex.StreamIterationTail;
import org.apache.flink.streaming.api.streamvertex.StreamVertex;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.state.OperatorState;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object for building Apache Flink stream processing graphs
 */
public class StreamGraph extends StreamingPlan {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraph.class);
	private final static String DEAFULT_JOB_NAME = "Flink Streaming Job";

	protected boolean chaining = true;
	private String jobName = DEAFULT_JOB_NAME;

	// Graph attributes
	private Map<String, Integer> operatorParallelisms;
	private Map<String, Long> bufferTimeouts;
	private Map<String, List<String>> outEdgeLists;
	private Map<String, List<Integer>> outEdgeTypes;
	private Map<String, List<List<String>>> selectedNames;
	private Map<String, List<String>> inEdgeLists;
	private Map<String, List<StreamPartitioner<?>>> outputPartitioners;
	private Map<String, String> operatorNames;
	private Map<String, StreamInvokable<?, ?>> invokableObjects;
	private Map<String, StreamRecordSerializer<?>> typeSerializersIn1;
	private Map<String, StreamRecordSerializer<?>> typeSerializersIn2;
	private Map<String, StreamRecordSerializer<?>> typeSerializersOut1;
	private Map<String, StreamRecordSerializer<?>> typeSerializersOut2;
	private Map<String, Class<? extends AbstractInvokable>> jobVertexClasses;
	private Map<String, List<OutputSelector<?>>> outputSelectors;
	private Map<String, Integer> iterationIds;
	private Map<Integer, String> iterationIDtoHeadName;
	private Map<Integer, String> iterationIDtoTailName;
	private Map<String, Integer> iterationTailCount;
	private Map<String, Long> iterationTimeouts;
	private Map<String, Map<String, OperatorState<?>>> operatorStates;
	private Map<String, InputFormat<String, ?>> inputFormatLists;

	private Set<String> sources;

	public StreamGraph() {

		initGraph();

		if (LOG.isDebugEnabled()) {
			LOG.debug("StreamGraph created");
		}
	}

	public void initGraph() {
		operatorParallelisms = new HashMap<String, Integer>();
		bufferTimeouts = new HashMap<String, Long>();
		outEdgeLists = new HashMap<String, List<String>>();
		outEdgeTypes = new HashMap<String, List<Integer>>();
		selectedNames = new HashMap<String, List<List<String>>>();
		inEdgeLists = new HashMap<String, List<String>>();
		outputPartitioners = new HashMap<String, List<StreamPartitioner<?>>>();
		operatorNames = new HashMap<String, String>();
		invokableObjects = new HashMap<String, StreamInvokable<?, ?>>();
		typeSerializersIn1 = new HashMap<String, StreamRecordSerializer<?>>();
		typeSerializersIn2 = new HashMap<String, StreamRecordSerializer<?>>();
		typeSerializersOut1 = new HashMap<String, StreamRecordSerializer<?>>();
		typeSerializersOut2 = new HashMap<String, StreamRecordSerializer<?>>();
		outputSelectors = new HashMap<String, List<OutputSelector<?>>>();
		jobVertexClasses = new HashMap<String, Class<? extends AbstractInvokable>>();
		iterationIds = new HashMap<String, Integer>();
		iterationIDtoHeadName = new HashMap<Integer, String>();
		iterationIDtoTailName = new HashMap<Integer, String>();
		iterationTailCount = new HashMap<String, Integer>();
		iterationTimeouts = new HashMap<String, Long>();
		operatorStates = new HashMap<String, Map<String, OperatorState<?>>>();
		inputFormatLists = new HashMap<String, InputFormat<String, ?>>();
		sources = new HashSet<String>();
	}

	/**
	 * Adds a vertex to the streaming graph with the given parameters
	 * 
	 * @param vertexName
	 *            Name of the vertex
	 * @param invokableObject
	 *            User defined operator
	 * @param inTypeInfo
	 *            Input type for serialization
	 * @param outTypeInfo
	 *            Output type for serialization
	 * @param operatorName
	 *            Operator type
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public <IN, OUT> void addStreamVertex(String vertexName,
			StreamInvokable<IN, OUT> invokableObject, TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo, String operatorName, int parallelism) {

		addVertex(vertexName, StreamVertex.class, invokableObject, operatorName, parallelism);

		StreamRecordSerializer<IN> inSerializer = inTypeInfo != null ? new StreamRecordSerializer<IN>(
				inTypeInfo) : null;
		StreamRecordSerializer<OUT> outSerializer = outTypeInfo != null ? new StreamRecordSerializer<OUT>(
				outTypeInfo) : null;

		addTypeSerializers(vertexName, inSerializer, null, outSerializer, null);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex: {}", vertexName);
		}
	}

	public <IN, OUT> void addSourceVertex(String vertexName,
			StreamInvokable<IN, OUT> invokableObject, TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo, String operatorName, int parallelism) {
		addStreamVertex(vertexName, invokableObject, inTypeInfo, outTypeInfo, operatorName,
				parallelism);
		sources.add(vertexName);
	}

	/**
	 * Adds a vertex for the iteration head to the {@link JobGraph}. The
	 * iterated values will be fed from this vertex back to the graph.
	 * 
	 * @param vertexName
	 *            Name of the vertex
	 * @param iterationHead
	 *            Id of the iteration head
	 * @param iterationID
	 *            ID of iteration for multiple iterations
	 * @param parallelism
	 *            Number of parallel instances created
	 * @param waitTime
	 *            Max wait time for next record
	 */
	public void addIterationHead(String vertexName, String iterationHead, Integer iterationID,
			int parallelism, long waitTime) {

		addVertex(vertexName, StreamIterationHead.class, null, null, parallelism);

		chaining = false;

		iterationIds.put(vertexName, iterationID);
		iterationIDtoHeadName.put(iterationID, vertexName);

		setSerializersFrom(iterationHead, vertexName);

		setEdge(vertexName, iterationHead,
				outputPartitioners.get(inEdgeLists.get(iterationHead).get(0)).get(0), 0,
				new ArrayList<String>());

		iterationTimeouts.put(iterationIDtoHeadName.get(iterationID), waitTime);

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SOURCE: {}", vertexName);
		}

		sources.add(vertexName);
	}

	/**
	 * Adds a vertex for the iteration tail to the {@link JobGraph}. The values
	 * intended to be iterated will be sent to this sink from the iteration
	 * head.
	 * 
	 * @param vertexName
	 *            Name of the vertex
	 * @param iterationTail
	 *            Id of the iteration tail
	 * @param iterationID
	 *            ID of iteration for mulitple iterations
	 * @param parallelism
	 *            Number of parallel instances created
	 * @param waitTime
	 *            Max waiting time for next record
	 */
	public void addIterationTail(String vertexName, String iterationTail, Integer iterationID,
			long waitTime) {

		if (bufferTimeouts.get(iterationTail) == 0) {
			throw new RuntimeException("Buffer timeout 0 at iteration tail is not supported.");
		}

		addVertex(vertexName, StreamIterationTail.class, null, null, getParallelism(iterationTail));

		iterationIds.put(vertexName, iterationID);
		iterationIDtoTailName.put(iterationID, vertexName);

		setSerializersFrom(iterationTail, vertexName);
		iterationTimeouts.put(iterationIDtoTailName.get(iterationID), waitTime);

		setParallelism(iterationIDtoHeadName.get(iterationID), getParallelism(iterationTail));
		setBufferTimeout(iterationIDtoHeadName.get(iterationID), bufferTimeouts.get(iterationTail));

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SINK: {}", vertexName);
		}

	}

	public <IN1, IN2, OUT> void addCoTask(String vertexName,
			CoInvokable<IN1, IN2, OUT> taskInvokableObject, TypeInformation<IN1> in1TypeInfo,
			TypeInformation<IN2> in2TypeInfo, TypeInformation<OUT> outTypeInfo,
			String operatorName, int parallelism) {

		addVertex(vertexName, CoStreamVertex.class, taskInvokableObject, operatorName, parallelism);

		addTypeSerializers(vertexName, new StreamRecordSerializer<IN1>(in1TypeInfo),
				new StreamRecordSerializer<IN2>(in2TypeInfo), new StreamRecordSerializer<OUT>(
						outTypeInfo), null);

		if (LOG.isDebugEnabled()) {
			LOG.debug("CO-TASK: {}", vertexName);
		}
	}

	/**
	 * Sets vertex parameters in the JobGraph
	 * 
	 * @param vertexName
	 *            Name of the vertex
	 * @param vertexClass
	 *            The class of the vertex
	 * @param invokableObjectject
	 *            The user defined invokable object
	 * @param operatorName
	 *            Type of the user defined operator
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	private void addVertex(String vertexName, Class<? extends AbstractInvokable> vertexClass,
			StreamInvokable<?, ?> invokableObject, String operatorName, int parallelism) {

		jobVertexClasses.put(vertexName, vertexClass);
		setParallelism(vertexName, parallelism);
		invokableObjects.put(vertexName, invokableObject);
		operatorNames.put(vertexName, operatorName);
		outEdgeLists.put(vertexName, new ArrayList<String>());
		outEdgeTypes.put(vertexName, new ArrayList<Integer>());
		selectedNames.put(vertexName, new ArrayList<List<String>>());
		outputSelectors.put(vertexName, new ArrayList<OutputSelector<?>>());
		inEdgeLists.put(vertexName, new ArrayList<String>());
		outputPartitioners.put(vertexName, new ArrayList<StreamPartitioner<?>>());
		iterationTailCount.put(vertexName, 0);
	}

	/**
	 * Connects two vertices in the JobGraph using the selected partitioner
	 * settings
	 * 
	 * @param upStreamVertexName
	 *            Name of the upstream(output) vertex
	 * @param downStreamVertexName
	 *            Name of the downstream(input) vertex
	 * @param partitionerObject
	 *            Partitioner object
	 * @param typeNumber
	 *            Number of the type (used at co-functions)
	 * @param outputNames
	 *            User defined names of the out edge
	 */
	public void setEdge(String upStreamVertexName, String downStreamVertexName,
			StreamPartitioner<?> partitionerObject, int typeNumber, List<String> outputNames) {
		outEdgeLists.get(upStreamVertexName).add(downStreamVertexName);
		outEdgeTypes.get(upStreamVertexName).add(typeNumber);
		inEdgeLists.get(downStreamVertexName).add(upStreamVertexName);
		outputPartitioners.get(upStreamVertexName).add(partitionerObject);
		selectedNames.get(upStreamVertexName).add(outputNames);
	}

	private void addTypeSerializers(String vertexName, StreamRecordSerializer<?> in1,
			StreamRecordSerializer<?> in2, StreamRecordSerializer<?> out1,
			StreamRecordSerializer<?> out2) {
		typeSerializersIn1.put(vertexName, in1);
		typeSerializersIn2.put(vertexName, in2);
		typeSerializersOut1.put(vertexName, out1);
		typeSerializersOut2.put(vertexName, out2);
	}

	/**
	 * Sets the number of parallel instances created for the given vertex.
	 * 
	 * @param vertexName
	 *            Name of the vertex
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public void setParallelism(String vertexName, int parallelism) {
		operatorParallelisms.put(vertexName, parallelism);
	}

	public int getParallelism(String vertexName) {
		return operatorParallelisms.get(vertexName);
	}

	/**
	 * Sets the input format for the given vertex.
	 * 
	 * @param vertexName
	 *            Name of the vertex
	 * @param inputFormat
	 *            input format of the file source associated with the given
	 *            vertex
	 */
	public void setInputFormat(String vertexName, InputFormat<String, ?> inputFormat) {
		inputFormatLists.put(vertexName, inputFormat);
	}

	public void setBufferTimeout(String vertexName, long bufferTimeout) {
		this.bufferTimeouts.put(vertexName, bufferTimeout);
	}

	public long getBufferTimeout(String vertexName) {
		return this.bufferTimeouts.get(vertexName);
	}

	public void addOperatorState(String veretxName, String stateName, OperatorState<?> state) {
		Map<String, OperatorState<?>> states = operatorStates.get(veretxName);
		if (states == null) {
			states = new HashMap<String, OperatorState<?>>();
			states.put(stateName, state);
		} else {
			if (states.containsKey(stateName)) {
				throw new RuntimeException("State has already been registered with this name: "
						+ stateName);
			} else {
				states.put(stateName, state);
			}
		}
		operatorStates.put(veretxName, states);
	}

	/**
	 * Sets a user defined {@link OutputSelector} for the given operator. Used
	 * for directed emits.
	 * 
	 * @param vertexName
	 *            Name of the vertex for which the output selector will be set
	 * @param outputSelector
	 *            The user defined output selector.
	 */
	public <T> void setOutputSelector(String vertexName, OutputSelector<T> outputSelector) {
		outputSelectors.get(vertexName).add(outputSelector);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Outputselector set for {}", vertexName);
		}

	}

	public <IN, OUT> void setInvokable(String vertexName, StreamInvokable<IN, OUT> invokableObject) {
		invokableObjects.put(vertexName, invokableObject);
	}

	public <OUT> void setOutType(String id, TypeInformation<OUT> outType) {
		StreamRecordSerializer<OUT> serializer = new StreamRecordSerializer<OUT>(outType);
		typeSerializersOut1.put(id, serializer);
	}

	public StreamInvokable<?, ?> getInvokable(String vertexName) {
		return invokableObjects.get(vertexName);
	}

	@SuppressWarnings("unchecked")
	public <OUT> StreamRecordSerializer<OUT> getOutSerializer1(String vertexName) {
		return (StreamRecordSerializer<OUT>) typeSerializersOut1.get(vertexName);
	}

	@SuppressWarnings("unchecked")
	public <OUT> StreamRecordSerializer<OUT> getOutSerializer2(String vertexName) {
		return (StreamRecordSerializer<OUT>) typeSerializersOut2.get(vertexName);
	}

	@SuppressWarnings("unchecked")
	public <IN> StreamRecordSerializer<IN> getInSerializer1(String vertexName) {
		return (StreamRecordSerializer<IN>) typeSerializersIn1.get(vertexName);
	}

	@SuppressWarnings("unchecked")
	public <IN> StreamRecordSerializer<IN> getInSerializer2(String vertexName) {
		return (StreamRecordSerializer<IN>) typeSerializersIn2.get(vertexName);
	}

	/**
	 * Sets TypeSerializerWrapper from one vertex to another, used with some
	 * sinks.
	 * 
	 * @param from
	 *            from
	 * @param to
	 *            to
	 */
	public void setSerializersFrom(String from, String to) {
		operatorNames.put(to, operatorNames.get(from));

		typeSerializersIn1.put(to, typeSerializersOut1.get(from));
		typeSerializersIn2.put(to, typeSerializersOut2.get(from));
		typeSerializersOut1.put(to, typeSerializersOut1.get(from));
		typeSerializersOut2.put(to, typeSerializersOut2.get(from));
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

		this.jobName = jobGraphName;
		StreamingJobGraphGenerator optimizer = new StreamingJobGraphGenerator(this);

		return optimizer.createJobGraph(jobGraphName);
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void setChaining(boolean chaining) {
		this.chaining = chaining;
	}

	public Collection<String> getSources() {
		return sources;
	}

	public List<String> getOutEdges(String vertexName) {
		return outEdgeLists.get(vertexName);
	}

	public List<String> getInEdges(String vertexName) {
		return inEdgeLists.get(vertexName);
	}

	public List<Integer> getOutEdgeTypes(String vertexName) {

		return outEdgeTypes.get(vertexName);
	}

	public StreamPartitioner<?> getOutPartitioner(String upStreamVertex, String downStreamVertex) {
		return outputPartitioners.get(upStreamVertex).get(
				outEdgeLists.get(upStreamVertex).indexOf(downStreamVertex));
	}

	public List<String> getSelectedNames(String upStreamVertex, String downStreamVertex) {

		return selectedNames.get(upStreamVertex).get(
				outEdgeLists.get(upStreamVertex).indexOf(downStreamVertex));
	}

	public Collection<Integer> getIterationIDs() {
		return new HashSet<Integer>(iterationIds.values());
	}

	public String getIterationTail(int iterID) {
		return iterationIDtoTailName.get(iterID);
	}

	public String getIterationHead(int iterID) {
		return iterationIDtoHeadName.get(iterID);
	}

	public Class<? extends AbstractInvokable> getJobVertexClass(String vertexName) {
		return jobVertexClasses.get(vertexName);
	}

	public InputFormat<String, ?> getInputFormat(String vertexName) {
		return inputFormatLists.get(vertexName);
	}

	public List<OutputSelector<?>> getOutputSelector(String vertexName) {
		return outputSelectors.get(vertexName);
	}

	public Map<String, OperatorState<?>> getState(String vertexName) {
		return operatorStates.get(vertexName);
	}

	public Integer getIterationID(String vertexName) {
		return iterationIds.get(vertexName);
	}

	public long getIterationTimeout(String vertexName) {
		return iterationTimeouts.get(vertexName);
	}

	public String getOperatorName(String vertexName) {
		return operatorNames.get(vertexName);
	}

	@Override
	public String getStreamingPlanAsJSON() {

		try {
			JSONObject json = new JSONObject();
			JSONArray nodes = new JSONArray();

			json.put("nodes", nodes);

			for (String id : operatorNames.keySet()) {
				JSONObject node = new JSONObject();
				nodes.put(node);

				node.put("id", Integer.valueOf(id));
				node.put("type", getOperatorName(id));

				if (sources.contains(id)) {
					node.put("pact", "Data Source");
				} else {
					node.put("pact", "Data Stream");
				}

				node.put("contents", getOperatorName(id) + " at "
						+ getInvokable(id).getUserFunction().getClass().getSimpleName());
				node.put("parallelism", getParallelism(id));

				int numIn = getInEdges(id).size();
				if (numIn > 0) {

					JSONArray inputs = new JSONArray();
					node.put("predecessors", inputs);

					for (int i = 0; i < numIn; i++) {

						String inID = getInEdges(id).get(i);

						JSONObject input = new JSONObject();
						inputs.put(input);

						input.put("id", Integer.valueOf(inID));
						input.put("ship_strategy", getOutPartitioner(inID, id).getStrategy());
						if (i == 0) {
							input.put("side", "first");
						} else if (i == 1) {
							input.put("side", "second");
						}
					}
				}

			}
			return json.toString();
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
}
