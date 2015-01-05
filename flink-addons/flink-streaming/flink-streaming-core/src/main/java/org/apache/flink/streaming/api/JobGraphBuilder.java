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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.SourceInvokable;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.api.streamvertex.CoStreamVertex;
import org.apache.flink.streaming.api.streamvertex.StreamIterationHead;
import org.apache.flink.streaming.api.streamvertex.StreamIterationTail;
import org.apache.flink.streaming.api.streamvertex.StreamVertex;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.partitioner.StreamPartitioner.PartitioningStrategy;
import org.apache.flink.streaming.state.OperatorState;
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
	private Map<String, List<String>> inEdgeList;
	private Map<String, List<StreamPartitioner<?>>> connectionTypes;
	private Map<String, String> operatorNames;
	private Map<String, StreamInvokable<?, ?>> invokableObjects;
	private Map<String, StreamRecordSerializer<?>> typeSerializersIn1;
	private Map<String, StreamRecordSerializer<?>> typeSerializersIn2;
	private Map<String, StreamRecordSerializer<?>> typeSerializersOut1;
	private Map<String, StreamRecordSerializer<?>> typeSerializersOut2;
	private Map<String, byte[]> outputSelectors;
	private Map<String, Class<? extends AbstractInvokable>> vertexClasses;
	private Map<String, Integer> iterationIds;
	private Map<Integer, String> iterationIDtoHeadName;
	private Map<Integer, String> iterationIDtoTailName;
	private Map<String, Integer> iterationTailCount;
	private Map<String, Long> iterationWaitTime;
	private Map<String, Map<String, OperatorState<?>>> operatorStates;
	private Map<String, InputFormat<String, ?>> inputFormatList;

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
		inEdgeList = new HashMap<String, List<String>>();
		connectionTypes = new HashMap<String, List<StreamPartitioner<?>>>();
		operatorNames = new HashMap<String, String>();
		invokableObjects = new HashMap<String, StreamInvokable<?, ?>>();
		typeSerializersIn1 = new HashMap<String, StreamRecordSerializer<?>>();
		typeSerializersIn2 = new HashMap<String, StreamRecordSerializer<?>>();
		typeSerializersOut1 = new HashMap<String, StreamRecordSerializer<?>>();
		typeSerializersOut2 = new HashMap<String, StreamRecordSerializer<?>>();
		outputSelectors = new HashMap<String, byte[]>();
		vertexClasses = new HashMap<String, Class<? extends AbstractInvokable>>();
		iterationIds = new HashMap<String, Integer>();
		iterationIDtoHeadName = new HashMap<Integer, String>();
		iterationIDtoTailName = new HashMap<Integer, String>();
		iterationTailCount = new HashMap<String, Integer>();
		iterationWaitTime = new HashMap<String, Long>();
		operatorStates = new HashMap<String, Map<String, OperatorState<?>>>();
		inputFormatList = new HashMap<String, InputFormat<String, ?>>();

		if (LOG.isDebugEnabled()) {
			LOG.debug("JobGraph created");
		}
	}

	/**
	 * Adds a vertex to the streaming JobGraph with the given parameters
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

	/**
	 * Adds a source vertex to the streaming JobGraph with the given parameters
	 * 
	 * @param vertexName
	 *            Name of the vertex
	 * @param function
	 *            User defined function
	 * @param inTypeInfo
	 *            Input type for serialization
	 * @param outTypeInfo
	 *            Output type for serialization
	 * @param operatorName
	 *            Operator type
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public <IN, OUT> void addSourceVertex(String vertexName, SourceFunction<OUT> function,
			TypeInformation<IN> inTypeInfo, TypeInformation<OUT> outTypeInfo, String operatorName,
			byte[] serializedFunction, int parallelism) {

		@SuppressWarnings("unchecked")
		StreamInvokable<IN, OUT> invokableObject = (StreamInvokable<IN, OUT>) new SourceInvokable<OUT>(
				function);

		addStreamVertex(vertexName, invokableObject, inTypeInfo, outTypeInfo, operatorName,
				parallelism);
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
	public void addIterationTail(String vertexName, String iterationTail, Integer iterationID,
			int parallelism, long waitTime) {

		if (bufferTimeout.get(iterationTail) == 0) {
			throw new RuntimeException("Buffer timeout 0 at iteration tail is not supported.");
		}

		addVertex(vertexName, StreamIterationTail.class, null, null, parallelism);

		iterationIds.put(vertexName, iterationID);
		iterationIDtoTailName.put(iterationID, vertexName);

		setBytesFrom(iterationTail, vertexName);
		iterationWaitTime.put(iterationIDtoTailName.get(iterationID), waitTime);

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

		vertexClasses.put(vertexName, vertexClass);
		setParallelism(vertexName, parallelism);
		invokableObjects.put(vertexName, invokableObject);
		operatorNames.put(vertexName, operatorName);
		outEdgeList.put(vertexName, new ArrayList<String>());
		outEdgeType.put(vertexName, new ArrayList<Integer>());
		outEdgeNames.put(vertexName, new ArrayList<List<String>>());
		outEdgeSelectAll.put(vertexName, new ArrayList<Boolean>());
		inEdgeList.put(vertexName, new ArrayList<String>());
		connectionTypes.put(vertexName, new ArrayList<StreamPartitioner<?>>());
		iterationTailCount.put(vertexName, 0);
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
		int parallelism = vertexParallelism.get(vertexName);
		byte[] outputSelector = outputSelectors.get(vertexName);
		Map<String, OperatorState<?>> state = operatorStates.get(vertexName);

		// Create vertex object
		AbstractJobVertex vertex = new AbstractJobVertex(vertexName);

		this.jobGraph.addVertex(vertex);

		vertex.setInvokableClass(vertexClass);
		vertex.setParallelism(parallelism);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, vertexName);
		}

		StreamConfig config = new StreamConfig(vertex.getConfiguration());

		config.setBufferTimeout(bufferTimeout.get(vertexName));

		config.setTypeSerializerIn1(typeSerializersIn1.get(vertexName));
		config.setTypeSerializerIn2(typeSerializersIn2.get(vertexName));
		config.setTypeSerializerOut1(typeSerializersOut1.get(vertexName));
		config.setTypeSerializerOut2(typeSerializersOut2.get(vertexName));

		// Set vertex config
		config.setUserInvokable(invokableObject);
		config.setVertexName(vertexName);
		config.setOutputSelector(outputSelector);
		config.setOperatorStates(state);

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(iterationIds.get(vertexName));
			config.setIterationWaitTime(iterationWaitTime.get(vertexName));
		}

		if (inputFormatList.containsKey(vertexName)) {
			vertex.setInputSplitSource(inputFormatList.get(vertexName));
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

		if (partitionerObject.getStrategy() == PartitioningStrategy.FORWARD) {
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
		inputFormatList.put(vertexName, inputFormat);
	}

	public void setBufferTimeout(String vertexName, long bufferTimeout) {
		this.bufferTimeout.put(vertexName, bufferTimeout);
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

	public <IN, OUT> void setInvokable(String id, StreamInvokable<IN, OUT> invokableObject) {
		invokableObjects.put(id, invokableObject);
	}

	public StreamInvokable<?, ?> getInvokable(String id) {
		return invokableObjects.get(id);
	}

	public <OUT> void setOutType(String id, TypeInformation<OUT> outType) {
		StreamRecordSerializer<OUT> serializer = new StreamRecordSerializer<OUT>(outType);
		typeSerializersOut1.put(id, serializer);
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
	public void setBytesFrom(String from, String to) {
		operatorNames.put(to, operatorNames.get(from));

		typeSerializersIn1.put(to, typeSerializersOut1.get(from));
		typeSerializersIn2.put(to, typeSerializersOut2.get(from));
		typeSerializersOut1.put(to, typeSerializersOut1.get(from));
		typeSerializersOut2.put(to, typeSerializersOut2.get(from));
	}

	/**
	 * Sets slot sharing for the vertices.
	 */
	private void setSlotSharing() {
		SlotSharingGroup shareGroup = new SlotSharingGroup();

		for (AbstractJobVertex vertex : streamVertices.values()) {
			vertex.setSlotSharingGroup(shareGroup);
		}

		for (Integer iterID : new HashSet<Integer>(iterationIds.values())) {
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
	 * @param jobGraphName
	 *            name of the jobGraph
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
