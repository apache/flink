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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamvertex.CoStreamVertex;
import org.apache.flink.streaming.api.streamvertex.StreamIterationHead;
import org.apache.flink.streaming.api.streamvertex.StreamIterationTail;
import org.apache.flink.streaming.api.streamvertex.StreamVertex;
import org.apache.flink.streaming.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.serialization.TypeWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object for building Apache Flink stream processing job graphs
 */
public class JobGraphBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(JobGraphBuilder.class);
	private final static String DEAFULT_JOB_NAME = "Streaming Job";
	private JobGraph jobGraph;

	// Graph attributes
	private Map<String, AbstractJobVertex> streamVertices;
	private Map<String, Integer> vertexParallelism;
	private Map<String, Long> bufferTimeout;
	private Map<String, List<String>> outEdgeList;
	private Map<String, List<Integer>> outEdgeType;
	private Map<String, List<List<String>>> outEdgeNames;
	private Map<String, List<Boolean>> outEdgeSelectAll;
	private Map<String, Boolean> mutability;
	private Map<String, List<String>> inEdgeList;
	private Map<String, List<StreamPartitioner<?>>> connectionTypes;
	private Map<String, String> operatorNames;
	private Map<String, StreamInvokable<?, ?>> invokableObjects;
	private Map<String, TypeWrapper<?>> typeWrapperIn1;
	private Map<String, TypeWrapper<?>> typeWrapperIn2;
	private Map<String, TypeWrapper<?>> typeWrapperOut1;
	private Map<String, TypeWrapper<?>> typeWrapperOut2;
	private Map<String, byte[]> serializedFunctions;
	private Map<String, byte[]> outputSelectors;
	private Map<String, Class<? extends AbstractInvokable>> vertexClasses;
	private Map<String, String> iterationIds;
	private Map<String, String> iterationIDtoHeadName;
	private Map<String, String> iterationIDtoTailName;
	private Map<String, Integer> iterationTailCount;
	private Map<String, Long> iterationWaitTime;

	private int degreeOfParallelism;
	private int executionParallelism;

	/**
	 * Creates an new {@link JobGraph} with the given name. A JobGraph is a DAG
	 * and consists of sources, tasks (intermediate vertices) and sinks.
	 */
	public JobGraphBuilder() {

		streamVertices = new HashMap<String, AbstractJobVertex>();
		vertexParallelism = new HashMap<String, Integer>();
		bufferTimeout = new HashMap<String, Long>();
		outEdgeList = new HashMap<String, List<String>>();
		outEdgeType = new HashMap<String, List<Integer>>();
		outEdgeNames = new HashMap<String, List<List<String>>>();
		outEdgeSelectAll = new HashMap<String, List<Boolean>>();
		mutability = new HashMap<String, Boolean>();
		inEdgeList = new HashMap<String, List<String>>();
		connectionTypes = new HashMap<String, List<StreamPartitioner<?>>>();
		operatorNames = new HashMap<String, String>();
		invokableObjects = new HashMap<String, StreamInvokable<?, ?>>();
		typeWrapperIn1 = new HashMap<String, TypeWrapper<?>>();
		typeWrapperIn2 = new HashMap<String, TypeWrapper<?>>();
		typeWrapperOut1 = new HashMap<String, TypeWrapper<?>>();
		typeWrapperOut2 = new HashMap<String, TypeWrapper<?>>();
		serializedFunctions = new HashMap<String, byte[]>();
		outputSelectors = new HashMap<String, byte[]>();
		vertexClasses = new HashMap<String, Class<? extends AbstractInvokable>>();
		iterationIds = new HashMap<String, String>();
		iterationIDtoHeadName = new HashMap<String, String>();
		iterationIDtoTailName = new HashMap<String, String>();
		iterationTailCount = new HashMap<String, Integer>();
		iterationWaitTime = new HashMap<String, Long>();

		if (LOG.isDebugEnabled()) {
			LOG.debug("JobGraph created");
		}
	}

	public int getDefaultParallelism() {
		return degreeOfParallelism;
	}

	public void setDefaultParallelism(int defaultParallelism) {
		this.degreeOfParallelism = defaultParallelism;
	}

	public int getExecutionParallelism() {
		return executionParallelism;
	}

	public void setExecutionParallelism(int executionParallelism) {
		this.executionParallelism = executionParallelism;
	}

	/**
	 * Adds a vertex to the streaming JobGraph with the given parameters
	 * 
	 * @param vertexName
	 *            Name of the vertex
	 * @param invokableObject
	 *            User defined operator
	 * @param inTypeWrapper
	 *            Input type wrapper for serialization
	 * @param outTypeWrapper
	 *            Output type wrapper for serialization
	 * @param operatorName
	 *            Operator type
	 * @param serializedFunction
	 *            Serialized udf
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public <IN, OUT> void addStreamVertex(String vertexName,
			StreamInvokable<IN, OUT> invokableObject, TypeWrapper<?> inTypeWrapper,
			TypeWrapper<?> outTypeWrapper, String operatorName, byte[] serializedFunction,
			int parallelism) {

		addVertex(vertexName, StreamVertex.class, invokableObject, operatorName,
				serializedFunction, parallelism);

		addTypeWrappers(vertexName, inTypeWrapper, null, outTypeWrapper, null);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex: {}", vertexName);
		}
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
	public void addIterationHead(String vertexName, String iterationHead, String iterationID,
			int parallelism, long waitTime) {

		addVertex(vertexName, StreamIterationHead.class, null, null, null, parallelism);

		iterationIds.put(vertexName, iterationID);
		iterationIDtoHeadName.put(iterationID, vertexName);

		setBytesFrom(iterationHead, vertexName);

		setEdge(vertexName, iterationHead, connectionTypes
				.get(inEdgeList.get(iterationHead).get(0)).get(0), 0, new ArrayList<String>(),
				false);

		iterationWaitTime.put(iterationIDtoHeadName.get(iterationID), waitTime);

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SOURCE: {}", vertexName);
		}
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
	public void addIterationTail(String vertexName, String iterationTail, String iterationID,
			int parallelism, long waitTime) {

		addVertex(vertexName, StreamIterationTail.class, null, null, null, parallelism);

		iterationIds.put(vertexName, iterationID);
		iterationIDtoTailName.put(iterationID, vertexName);

		setBytesFrom(iterationTail, vertexName);
		iterationWaitTime.put(iterationIDtoTailName.get(iterationID), waitTime);

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SINK: {}", vertexName);
		}

	}

	public <IN1, IN2, OUT> void addCoTask(String vertexName,
			CoInvokable<IN1, IN2, OUT> taskInvokableObject, TypeWrapper<?> in1TypeWrapper,
			TypeWrapper<?> in2TypeWrapper, TypeWrapper<?> outTypeWrapper, String operatorName,
			byte[] serializedFunction, int parallelism) {

		addVertex(vertexName, CoStreamVertex.class, taskInvokableObject, operatorName,
				serializedFunction, parallelism);

		addTypeWrappers(vertexName, in1TypeWrapper, in2TypeWrapper, outTypeWrapper, null);

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
	 * @param invokableObject
	 *            The user defined invokable object
	 * @param operatorName
	 *            Type of the user defined operator
	 * @param serializedFunction
	 *            Serialized operator
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	private void addVertex(String vertexName, Class<? extends AbstractInvokable> vertexClass,
			StreamInvokable<?, ?> invokableObject, String operatorName, byte[] serializedFunction,
			int parallelism) {

		vertexClasses.put(vertexName, vertexClass);
		setParallelism(vertexName, parallelism);
		mutability.put(vertexName, false);
		invokableObjects.put(vertexName, invokableObject);
		operatorNames.put(vertexName, operatorName);
		serializedFunctions.put(vertexName, serializedFunction);
		outEdgeList.put(vertexName, new ArrayList<String>());
		outEdgeType.put(vertexName, new ArrayList<Integer>());
		outEdgeNames.put(vertexName, new ArrayList<List<String>>());
		outEdgeSelectAll.put(vertexName, new ArrayList<Boolean>());
		inEdgeList.put(vertexName, new ArrayList<String>());
		connectionTypes.put(vertexName, new ArrayList<StreamPartitioner<?>>());
		iterationTailCount.put(vertexName, 0);
	}

	private void addTypeWrappers(String vertexName, TypeWrapper<?> in1, TypeWrapper<?> in2,
			TypeWrapper<?> out1, TypeWrapper<?> out2) {
		typeWrapperIn1.put(vertexName, in1);
		typeWrapperIn2.put(vertexName, in2);
		typeWrapperOut1.put(vertexName, out1);
		typeWrapperOut2.put(vertexName, out2);
	}

	/**
	 * Creates an {@link AbstractJobVertex} in the {@link JobGraph} and sets its
	 * config parameters using the ones set previously.
	 * 
	 * @param vertexName
	 *            Name for which the vertex will be created.
	 */
	private void createVertex(String vertexName) {

		// Get vertex attributes
		Class<? extends AbstractInvokable> vertexClass = vertexClasses.get(vertexName);
		StreamInvokable<?, ?> invokableObject = invokableObjects.get(vertexName);
		String operatorName = operatorNames.get(vertexName);
		byte[] serializedFunction = serializedFunctions.get(vertexName);
		int parallelism = vertexParallelism.get(vertexName);
		byte[] outputSelector = outputSelectors.get(vertexName);

		// Create vertex object
		AbstractJobVertex vertex = new AbstractJobVertex(vertexName);

		this.jobGraph.addVertex(vertex);

		vertex.setInvokableClass(vertexClass);
		vertex.setParallelism(parallelism);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, vertexName);
		}

		StreamConfig config = new StreamConfig(vertex.getConfiguration());

		config.setMutability(mutability.get(vertexName));
		config.setBufferTimeout(bufferTimeout.get(vertexName));

		config.setTypeWrapperIn1(typeWrapperIn1.get(vertexName));
		config.setTypeWrapperIn2(typeWrapperIn2.get(vertexName));
		config.setTypeWrapperOut1(typeWrapperOut1.get(vertexName));
		config.setTypeWrapperOut2(typeWrapperOut2.get(vertexName));

		// Set vertex config
		config.setUserInvokable(invokableObject);
		config.setVertexName(vertexName);
		config.setFunction(serializedFunction, operatorName);
		config.setOutputSelector(outputSelector);

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(iterationIds.get(vertexName));
			config.setIterationWaitTime(iterationWaitTime.get(vertexName));
		}

		streamVertices.put(vertexName, vertex);
	}

	/**
	 * Connects two vertices with the given names, partitioning and channel type
	 * 
	 * @param upStreamVertexName
	 *            Name of the upstream vertex, that will emit the values
	 * @param downStreamVertexName
	 *            Name of the downstream vertex, that will receive the values
	 * @param partitionerObject
	 *            The partitioner
	 */
	private <T> void connect(String upStreamVertexName, String downStreamVertexName,
			StreamPartitioner<T> partitionerObject) {

		AbstractJobVertex upStreamVertex = streamVertices.get(upStreamVertexName);
		AbstractJobVertex downStreamVertex = streamVertices.get(downStreamVertexName);

		StreamConfig config = new StreamConfig(upStreamVertex.getConfiguration());

		if (partitionerObject.getClass().equals(ForwardPartitioner.class)) {
			downStreamVertex
					.connectNewDataSetAsInput(upStreamVertex, DistributionPattern.POINTWISE);
		} else {
			downStreamVertex
					.connectNewDataSetAsInput(upStreamVertex, DistributionPattern.BIPARTITE);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitionerObject.getClass().getSimpleName(),
					upStreamVertexName, downStreamVertexName);
		}

		int outputIndex = upStreamVertex.getNumberOfProducedIntermediateDataSets() - 1;

		config.setOutputName(outputIndex, outEdgeNames.get(upStreamVertexName).get(outputIndex));
		config.setSelectAll(outputIndex, outEdgeSelectAll.get(upStreamVertexName).get(outputIndex));
		config.setPartitioner(outputIndex, partitionerObject);
		config.setNumberOfOutputChannels(outputIndex, vertexParallelism.get(downStreamVertexName));
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
		vertexParallelism.put(vertexName, parallelism);
	}

	public void setMutability(String vertexName, boolean isMutable) {
		mutability.put(vertexName, isMutable);
	}

	public void setBufferTimeout(String vertexName, long bufferTimeout) {
		this.bufferTimeout.put(vertexName, bufferTimeout);
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
			StreamPartitioner<?> partitionerObject, int typeNumber, List<String> outputNames,
			boolean selectAll) {
		outEdgeList.get(upStreamVertexName).add(downStreamVertexName);
		outEdgeType.get(upStreamVertexName).add(typeNumber);
		inEdgeList.get(downStreamVertexName).add(upStreamVertexName);
		connectionTypes.get(upStreamVertexName).add(partitionerObject);
		outEdgeNames.get(upStreamVertexName).add(outputNames);
		outEdgeSelectAll.get(upStreamVertexName).add(selectAll);
	}

	/**
	 * Sets the parallelism and buffertimeout of the iteration head of the given
	 * iteration id to the parallelism given.
	 * 
	 * @param iterationID
	 *            ID of the iteration
	 * @param iterationTail
	 *            ID of the iteration tail
	 */
	public void setIterationSourceSettings(String iterationID, String iterationTail) {
		setParallelism(iterationIDtoHeadName.get(iterationID), vertexParallelism.get(iterationTail));
		setBufferTimeout(iterationIDtoHeadName.get(iterationID), bufferTimeout.get(iterationTail));
	}

	/**
	 * Sets a user defined {@link OutputSelector} for the given vertex. Used for
	 * directed emits.
	 * 
	 * @param vertexName
	 *            Name of the vertex for which the output selector will be set
	 * @param serializedOutputSelector
	 *            Byte array representing the serialized output selector.
	 */
	public <T> void setOutputSelector(String vertexName, byte[] serializedOutputSelector) {
		outputSelectors.put(vertexName, serializedOutputSelector);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Outputselector set for {}", vertexName);
		}

	}

	/**
	 * Sets udf operator and TypeSerializerWrapper from one vertex to another,
	 * used with some sinks.
	 * 
	 * @param from
	 *            from
	 * @param to
	 *            to
	 */
	public void setBytesFrom(String from, String to) {
		operatorNames.put(to, operatorNames.get(from));
		serializedFunctions.put(to, serializedFunctions.get(from));

		typeWrapperIn1.put(to, typeWrapperOut1.get(from));
		typeWrapperIn2.put(to, typeWrapperOut2.get(from));
		typeWrapperOut1.put(to, typeWrapperOut1.get(from));
		typeWrapperOut2.put(to, typeWrapperOut2.get(from));
	}

	public TypeInformation<?> getInTypeInfo(String id) {
		System.out.println("DEBUG TypeInfo " + typeWrapperIn1.get(id));
		return typeWrapperIn1.get(id).getTypeInfo();
	}

	public TypeInformation<?> getOutTypeInfo(String id) {
		return typeWrapperOut1.get(id).getTypeInfo();
	}

	/**
	 * Sets slot sharing for the vertices.
	 */
	private void setSlotSharing() {
		SlotSharingGroup shareGroup = new SlotSharingGroup();

		for (AbstractJobVertex vertex : streamVertices.values()) {
			vertex.setSlotSharingGroup(shareGroup);
		}

		for (String iterID : new HashSet<String>(iterationIds.values())) {
			CoLocationGroup ccg = new CoLocationGroup();
			AbstractJobVertex tail = streamVertices.get(iterationIDtoTailName.get(iterID));
			AbstractJobVertex head = streamVertices.get(iterationIDtoHeadName.get(iterID));

			ccg.addVertex(head);
			ccg.addVertex(tail);
		}
	}

	/**
	 * Writes number of inputs into each JobVertex's config
	 */
	private void setNumberOfJobInputs() {
		for (AbstractJobVertex vertex : streamVertices.values()) {
			(new StreamConfig(vertex.getConfiguration())).setNumberOfInputs(vertex
					.getNumberOfInputs());
		}
	}

	/**
	 * Writes the number of outputs and output channels into each JobVertex's
	 * config
	 */
	private void setNumberOfJobOutputs() {
		for (AbstractJobVertex vertex : streamVertices.values()) {
			(new StreamConfig(vertex.getConfiguration())).setNumberOfOutputs(vertex
					.getNumberOfProducedIntermediateDataSets());
		}
	}

	/**
	 * Gets the assembled {@link JobGraph} and adds a default name for it.
	 */
	public JobGraph getJobGraph() {
		return getJobGraph(DEAFULT_JOB_NAME);
	}

	/**
	 * Gets the assembled {@link JobGraph} and adds a user specified name for
	 * it.
	 * 
	 * @param jobGraphName name of the jobGraph
	 */
	public JobGraph getJobGraph(String jobGraphName) {
		jobGraph = new JobGraph(jobGraphName);
		buildJobGraph();
		return jobGraph;
	}

	/**
	 * Builds the {@link JobGraph} from the vertices with the edges and settings
	 * provided.
	 */
	private void buildJobGraph() {
		for (String vertexName : outEdgeList.keySet()) {
			createVertex(vertexName);
		}

		for (String upStreamVertexName : outEdgeList.keySet()) {
			int i = 0;

			List<Integer> outEdgeTypeList = outEdgeType.get(upStreamVertexName);

			for (String downStreamVertexName : outEdgeList.get(upStreamVertexName)) {
				StreamConfig downStreamVertexConfig = new StreamConfig(streamVertices.get(
						downStreamVertexName).getConfiguration());

				int inputNumber = downStreamVertexConfig.getNumberOfInputs();

				downStreamVertexConfig.setInputType(inputNumber++, outEdgeTypeList.get(i));
				downStreamVertexConfig.setNumberOfInputs(inputNumber);

				connect(upStreamVertexName, downStreamVertexName,
						connectionTypes.get(upStreamVertexName).get(i));
				i++;
			}
		}

		setSlotSharing();
		setNumberOfJobInputs();
		setNumberOfJobOutputs();
	}

}
