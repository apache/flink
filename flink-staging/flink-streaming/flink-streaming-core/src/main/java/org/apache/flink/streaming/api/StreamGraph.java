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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.common.ExecutionConfig;
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
	private Map<Integer, Integer> operatorParallelisms;
	private Map<Integer, Long> bufferTimeouts;
	private Map<Integer, List<Integer>> outEdgeLists;
	private Map<Integer, List<Integer>> outEdgeTypes;
	private Map<Integer, List<List<String>>> selectedNames;
	private Map<Integer, List<Integer>> inEdgeLists;
	private Map<Integer, List<StreamPartitioner<?>>> outputPartitioners;
	private Map<Integer, String> operatorNames;
	private Map<Integer, StreamInvokable<?, ?>> invokableObjects;
	private Map<Integer, StreamRecordSerializer<?>> typeSerializersIn1;
	private Map<Integer, StreamRecordSerializer<?>> typeSerializersIn2;
	private Map<Integer, StreamRecordSerializer<?>> typeSerializersOut1;
	private Map<Integer, StreamRecordSerializer<?>> typeSerializersOut2;
	private Map<Integer, Class<? extends AbstractInvokable>> jobVertexClasses;
	private Map<Integer, List<OutputSelector<?>>> outputSelectors;
	private Map<Integer, Integer> iterationIds;
	private Map<Integer, Integer> iterationIDtoHeadID;
	private Map<Integer, Integer> iterationIDtoTailID;
	private Map<Integer, Integer> iterationTailCount;
	private Map<Integer, Long> iterationTimeouts;
	private Map<Integer, Map<String, OperatorState<?>>> operatorStates;
	private Map<Integer, InputFormat<String, ?>> inputFormatLists;
	private List<Map<Integer, ?>> containingMaps;

	private Set<Integer> sources;

	private ExecutionConfig executionConfig;

	public StreamGraph(ExecutionConfig executionConfig) {

		this.executionConfig = executionConfig;

		initGraph();

		if (LOG.isDebugEnabled()) {
			LOG.debug("StreamGraph created");
		}
	}

	public void initGraph() {
		containingMaps = new ArrayList<Map<Integer, ?>>();

		operatorParallelisms = new HashMap<Integer, Integer>();
		containingMaps.add(operatorParallelisms);
		bufferTimeouts = new HashMap<Integer, Long>();
		containingMaps.add(bufferTimeouts);
		outEdgeLists = new HashMap<Integer, List<Integer>>();
		containingMaps.add(outEdgeLists);
		outEdgeTypes = new HashMap<Integer, List<Integer>>();
		containingMaps.add(outEdgeTypes);
		selectedNames = new HashMap<Integer, List<List<String>>>();
		containingMaps.add(selectedNames);
		inEdgeLists = new HashMap<Integer, List<Integer>>();
		containingMaps.add(inEdgeLists);
		outputPartitioners = new HashMap<Integer, List<StreamPartitioner<?>>>();
		containingMaps.add(outputPartitioners);
		operatorNames = new HashMap<Integer, String>();
		containingMaps.add(operatorNames);
		invokableObjects = new HashMap<Integer, StreamInvokable<?, ?>>();
		containingMaps.add(invokableObjects);
		typeSerializersIn1 = new HashMap<Integer, StreamRecordSerializer<?>>();
		containingMaps.add(typeSerializersIn1);
		typeSerializersIn2 = new HashMap<Integer, StreamRecordSerializer<?>>();
		containingMaps.add(typeSerializersIn2);
		typeSerializersOut1 = new HashMap<Integer, StreamRecordSerializer<?>>();
		containingMaps.add(typeSerializersOut1);
		typeSerializersOut2 = new HashMap<Integer, StreamRecordSerializer<?>>();
		containingMaps.add(typeSerializersOut1);
		outputSelectors = new HashMap<Integer, List<OutputSelector<?>>>();
		containingMaps.add(outputSelectors);
		jobVertexClasses = new HashMap<Integer, Class<? extends AbstractInvokable>>();
		containingMaps.add(jobVertexClasses);
		iterationIds = new HashMap<Integer, Integer>();
		containingMaps.add(jobVertexClasses);
		iterationIDtoHeadID = new HashMap<Integer, Integer>();
		iterationIDtoTailID = new HashMap<Integer, Integer>();
		iterationTailCount = new HashMap<Integer, Integer>();
		containingMaps.add(iterationTailCount);
		iterationTimeouts = new HashMap<Integer, Long>();
		containingMaps.add(iterationTailCount);
		operatorStates = new HashMap<Integer, Map<String, OperatorState<?>>>();
		containingMaps.add(operatorStates);
		inputFormatLists = new HashMap<Integer, InputFormat<String, ?>>();
		containingMaps.add(operatorStates);
		sources = new HashSet<Integer>();
	}

	/**
	 * Adds a vertex to the streaming graph with the given parameters
	 * 
	 * @param vertexID
	 *            ID of the vertex
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
	public <IN, OUT> void addStreamVertex(Integer vertexID,
			StreamInvokable<IN, OUT> invokableObject, TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo, String operatorName, int parallelism) {

		addVertex(vertexID, StreamVertex.class, invokableObject, operatorName, parallelism);

		StreamRecordSerializer<IN> inSerializer = inTypeInfo != null ? new StreamRecordSerializer<IN>(
				inTypeInfo, executionConfig) : null;
		StreamRecordSerializer<OUT> outSerializer = outTypeInfo != null ? new StreamRecordSerializer<OUT>(
				outTypeInfo, executionConfig) : null;

		addTypeSerializers(vertexID, inSerializer, null, outSerializer, null);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex: {}", vertexID);
		}
	}

	public <IN, OUT> void addSourceVertex(Integer vertexID,
			StreamInvokable<IN, OUT> invokableObject, TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo, String operatorName, int parallelism) {
		addStreamVertex(vertexID, invokableObject, inTypeInfo, outTypeInfo, operatorName,
				parallelism);
		sources.add(vertexID);
	}

	/**
	 * Adds a vertex for the iteration head to the {@link JobGraph}. The
	 * iterated values will be fed from this vertex back to the graph.
	 * 
	 * @param vertexID
	 *            ID of the vertex
	 * @param iterationHead
	 *            Id of the iteration head
	 * @param iterationID
	 *            ID of iteration for multiple iterations
	 * @param parallelism
	 *            Number of parallel instances created
	 * @param waitTime
	 *            Max wait time for next record
	 */
	public void addIterationHead(Integer vertexID, Integer iterationHead, Integer iterationID,
			int parallelism, long waitTime) {

		addVertex(vertexID, StreamIterationHead.class, null, null, parallelism);

		chaining = false;

		iterationIds.put(vertexID, iterationID);
		iterationIDtoHeadID.put(iterationID, vertexID);

		setSerializersFrom(iterationHead, vertexID);

		setEdge(vertexID, iterationHead,
				outputPartitioners.get(inEdgeLists.get(iterationHead).get(0)).get(0), 0,
				new ArrayList<String>());

		iterationTimeouts.put(iterationIDtoHeadID.get(iterationID), waitTime);

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SOURCE: {}", vertexID);
		}

		sources.add(vertexID);
	}

	/**
	 * Adds a vertex for the iteration tail to the {@link JobGraph}. The values
	 * intended to be iterated will be sent to this sink from the iteration
	 * head.
	 * 
	 * @param vertexID
	 *            ID of the vertex
	 * @param iterationTail
	 *            Id of the iteration tail
	 * @param iterationID
	 *            ID of iteration for mulitple iterations
	 * @param parallelism
	 *            Number of parallel instances created
	 * @param waitTime
	 *            Max waiting time for next record
	 */
	public void addIterationTail(Integer vertexID, Integer iterationTail, Integer iterationID,
			long waitTime) {

		if (bufferTimeouts.get(iterationTail) == 0) {
			throw new RuntimeException("Buffer timeout 0 at iteration tail is not supported.");
		}

		addVertex(vertexID, StreamIterationTail.class, null, null, getParallelism(iterationTail));

		iterationIds.put(vertexID, iterationID);
		iterationIDtoTailID.put(iterationID, vertexID);

		setSerializersFrom(iterationTail, vertexID);
		iterationTimeouts.put(iterationIDtoTailID.get(iterationID), waitTime);

		setParallelism(iterationIDtoHeadID.get(iterationID), getParallelism(iterationTail));
		setBufferTimeout(iterationIDtoHeadID.get(iterationID), bufferTimeouts.get(iterationTail));

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SINK: {}", vertexID);
		}

	}

	public <IN1, IN2, OUT> void addCoTask(Integer vertexID,
			CoInvokable<IN1, IN2, OUT> taskInvokableObject, TypeInformation<IN1> in1TypeInfo,
			TypeInformation<IN2> in2TypeInfo, TypeInformation<OUT> outTypeInfo,
			String operatorName, int parallelism) {

		addVertex(vertexID, CoStreamVertex.class, taskInvokableObject, operatorName, parallelism);

		addTypeSerializers(vertexID, new StreamRecordSerializer<IN1>(in1TypeInfo, executionConfig),
				new StreamRecordSerializer<IN2>(in2TypeInfo, executionConfig),
				new StreamRecordSerializer<OUT>(outTypeInfo, executionConfig), null);

		if (LOG.isDebugEnabled()) {
			LOG.debug("CO-TASK: {}", vertexID);
		}
	}

	/**
	 * Sets vertex parameters in the JobGraph
	 * 
	 * @param vertexID
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
	private void addVertex(Integer vertexID, Class<? extends AbstractInvokable> vertexClass,
			StreamInvokable<?, ?> invokableObject, String operatorName, int parallelism) {

		jobVertexClasses.put(vertexID, vertexClass);
		setParallelism(vertexID, parallelism);
		invokableObjects.put(vertexID, invokableObject);
		operatorNames.put(vertexID, operatorName);
		outEdgeLists.put(vertexID, new ArrayList<Integer>());
		outEdgeTypes.put(vertexID, new ArrayList<Integer>());
		selectedNames.put(vertexID, new ArrayList<List<String>>());
		outputSelectors.put(vertexID, new ArrayList<OutputSelector<?>>());
		inEdgeLists.put(vertexID, new ArrayList<Integer>());
		outputPartitioners.put(vertexID, new ArrayList<StreamPartitioner<?>>());
		iterationTailCount.put(vertexID, 0);
	}

	/**
	 * Connects two vertices in the JobGraph using the selected partitioner
	 * settings
	 * 
	 * @param upStreamVertexID
	 *            ID of the upstream(output) vertex
	 * @param downStreamVertexID
	 *            ID of the downstream(input) vertex
	 * @param partitionerObject
	 *            Partitioner object
	 * @param typeNumber
	 *            Number of the type (used at co-functions)
	 * @param outputNames
	 *            User defined names of the out edge
	 */
	public void setEdge(Integer upStreamVertexID, Integer downStreamVertexID,
			StreamPartitioner<?> partitionerObject, int typeNumber, List<String> outputNames) {
		outEdgeLists.get(upStreamVertexID).add(downStreamVertexID);
		outEdgeTypes.get(upStreamVertexID).add(typeNumber);
		inEdgeLists.get(downStreamVertexID).add(upStreamVertexID);
		outputPartitioners.get(upStreamVertexID).add(partitionerObject);
		selectedNames.get(upStreamVertexID).add(outputNames);
	}

	public void removeEdge(Integer upStream, Integer downStream) {
		int inputIndex = getInEdges(downStream).indexOf(upStream);
		inEdgeLists.get(downStream).remove(inputIndex);

		int outputIndex = getOutEdges(upStream).indexOf(downStream);
		outEdgeLists.get(upStream).remove(outputIndex);
		outEdgeTypes.get(upStream).remove(outputIndex);
		selectedNames.get(upStream).remove(outputIndex);
		outputPartitioners.get(upStream).remove(outputIndex);
	}

	public void removeVertex(Integer toRemove) {
		List<Integer> outEdges = new ArrayList<Integer>(getOutEdges(toRemove));
		List<Integer> inEdges = new ArrayList<Integer>(getInEdges(toRemove));

		for (Integer output : outEdges) {
			removeEdge(toRemove, output);
		}

		for (Integer input : inEdges) {
			removeEdge(input, toRemove);
		}

		for (Map<Integer, ?> map : containingMaps) {
			map.remove(toRemove);
		}

	}

	private void addTypeSerializers(Integer vertexID, StreamRecordSerializer<?> in1,
			StreamRecordSerializer<?> in2, StreamRecordSerializer<?> out1,
			StreamRecordSerializer<?> out2) {
		typeSerializersIn1.put(vertexID, in1);
		typeSerializersIn2.put(vertexID, in2);
		typeSerializersOut1.put(vertexID, out1);
		typeSerializersOut2.put(vertexID, out2);
	}

	/**
	 * Sets the number of parallel instances created for the given vertex.
	 * 
	 * @param vertexID
	 *            ID of the vertex
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public void setParallelism(Integer vertexID, int parallelism) {
		operatorParallelisms.put(vertexID, parallelism);
	}

	public int getParallelism(Integer vertexID) {
		return operatorParallelisms.get(vertexID);
	}

	/**
	 * Sets the input format for the given vertex.
	 * 
	 * @param vertexID
	 *            Name of the vertex
	 * @param inputFormat
	 *            input format of the file source associated with the given
	 *            vertex
	 */
	public void setInputFormat(Integer vertexID, InputFormat<String, ?> inputFormat) {
		inputFormatLists.put(vertexID, inputFormat);
	}

	public void setBufferTimeout(Integer vertexID, long bufferTimeout) {
		this.bufferTimeouts.put(vertexID, bufferTimeout);
	}

	public long getBufferTimeout(Integer vertexID) {
		return this.bufferTimeouts.get(vertexID);
	}

	public void addOperatorState(Integer veretxName, String stateName, OperatorState<?> state) {
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
	 * @param vertexID
	 *            Name of the vertex for which the output selector will be set
	 * @param outputSelector
	 *            The user defined output selector.
	 */
	public <T> void setOutputSelector(Integer vertexID, OutputSelector<T> outputSelector) {
		outputSelectors.get(vertexID).add(outputSelector);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Outputselector set for {}", vertexID);
		}

	}

	public <IN, OUT> void setInvokable(Integer vertexID, StreamInvokable<IN, OUT> invokableObject) {
		invokableObjects.put(vertexID, invokableObject);
	}

	public <OUT> void setOutType(Integer id, TypeInformation<OUT> outType) {
		StreamRecordSerializer<OUT> serializer = new StreamRecordSerializer<OUT>(outType,
				executionConfig);
		typeSerializersOut1.put(id, serializer);
	}

	public StreamInvokable<?, ?> getInvokable(Integer vertexID) {
		return invokableObjects.get(vertexID);
	}

	@SuppressWarnings("unchecked")
	public <OUT> StreamRecordSerializer<OUT> getOutSerializer1(Integer vertexID) {
		return (StreamRecordSerializer<OUT>) typeSerializersOut1.get(vertexID);
	}

	@SuppressWarnings("unchecked")
	public <OUT> StreamRecordSerializer<OUT> getOutSerializer2(Integer vertexID) {
		return (StreamRecordSerializer<OUT>) typeSerializersOut2.get(vertexID);
	}

	@SuppressWarnings("unchecked")
	public <IN> StreamRecordSerializer<IN> getInSerializer1(Integer vertexID) {
		return (StreamRecordSerializer<IN>) typeSerializersIn1.get(vertexID);
	}

	@SuppressWarnings("unchecked")
	public <IN> StreamRecordSerializer<IN> getInSerializer2(Integer vertexID) {
		return (StreamRecordSerializer<IN>) typeSerializersIn2.get(vertexID);
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
	public void setSerializersFrom(Integer from, Integer to) {
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

		WindowingOptimzier.optimizeGraph(this);

		StreamingJobGraphGenerator jobgraphGenerator = new StreamingJobGraphGenerator(this);

		return jobgraphGenerator.createJobGraph(jobGraphName);
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void setChaining(boolean chaining) {
		this.chaining = chaining;
	}

	public Set<Entry<Integer, StreamInvokable<?, ?>>> getInvokables() {
		return invokableObjects.entrySet();
	}

	public Collection<Integer> getSources() {
		return sources;
	}

	public List<Integer> getOutEdges(Integer vertexID) {
		return outEdgeLists.get(vertexID);
	}

	public List<Integer> getInEdges(Integer vertexID) {
		return inEdgeLists.get(vertexID);
	}

	public List<Integer> getOutEdgeTypes(Integer vertexID) {

		return outEdgeTypes.get(vertexID);
	}

	public StreamPartitioner<?> getOutPartitioner(Integer upStreamVertex, Integer downStreamVertex) {
		return outputPartitioners.get(upStreamVertex).get(
				outEdgeLists.get(upStreamVertex).indexOf(downStreamVertex));
	}

	public List<String> getSelectedNames(Integer upStreamVertex, Integer downStreamVertex) {

		return selectedNames.get(upStreamVertex).get(
				outEdgeLists.get(upStreamVertex).indexOf(downStreamVertex));
	}

	public Collection<Integer> getIterationIDs() {
		return new HashSet<Integer>(iterationIds.values());
	}

	public Integer getIterationTail(int iterID) {
		return iterationIDtoTailID.get(iterID);
	}

	public Integer getIterationHead(int iterID) {
		return iterationIDtoHeadID.get(iterID);
	}

	public Class<? extends AbstractInvokable> getJobVertexClass(Integer vertexID) {
		return jobVertexClasses.get(vertexID);
	}

	public InputFormat<String, ?> getInputFormat(Integer vertexID) {
		return inputFormatLists.get(vertexID);
	}

	public List<OutputSelector<?>> getOutputSelector(Integer vertexID) {
		return outputSelectors.get(vertexID);
	}

	public Map<String, OperatorState<?>> getState(Integer vertexID) {
		return operatorStates.get(vertexID);
	}

	public Integer getIterationID(Integer vertexID) {
		return iterationIds.get(vertexID);
	}

	public long getIterationTimeout(Integer vertexID) {
		return iterationTimeouts.get(vertexID);
	}

	public String getOperatorName(Integer vertexID) {
		return operatorNames.get(vertexID);
	}

	@Override
	public String getStreamingPlanAsJSON() {

		WindowingOptimzier.optimizeGraph(this);

		try {
			JSONObject json = new JSONObject();
			JSONArray nodes = new JSONArray();

			json.put("nodes", nodes);
			List<Integer> operatorIDs = new ArrayList<Integer>(operatorNames.keySet());
			Collections.sort(operatorIDs);

			for (Integer id : operatorIDs) {
				JSONObject node = new JSONObject();
				nodes.put(node);

				node.put("id", id);
				node.put("type", getOperatorName(id));

				if (sources.contains(id)) {
					node.put("pact", "Data Source");
				} else {
					node.put("pact", "Data Stream");
				}

				if (getInvokable(id) != null && getInvokable(id).getUserFunction() != null) {
					node.put("contents", getOperatorName(id) + " at "
							+ getInvokable(id).getUserFunction().getClass().getSimpleName());
				} else {
					node.put("contents", getOperatorName(id));
				}

				node.put("parallelism", getParallelism(id));

				int numIn = getInEdges(id).size();
				if (numIn > 0) {

					JSONArray inputs = new JSONArray();
					node.put("predecessors", inputs);

					for (int i = 0; i < numIn; i++) {

						Integer inID = getInEdges(id).get(i);

						JSONObject input = new JSONObject();
						inputs.put(input);

						input.put("id", inID);
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
