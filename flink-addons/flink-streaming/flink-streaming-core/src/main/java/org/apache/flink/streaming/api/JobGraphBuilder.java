/**
 *
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
 *
 */

package org.apache.flink.streaming.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.io.network.channels.ChannelType;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphDefinitionException;
import org.apache.flink.runtime.jobgraph.JobInputVertex;
import org.apache.flink.runtime.jobgraph.JobOutputVertex;
import org.apache.flink.runtime.jobgraph.JobTaskVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.invokable.SinkInvokable;
import org.apache.flink.streaming.api.invokable.SourceInvokable;
import org.apache.flink.streaming.api.invokable.StreamComponentInvokable;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamcomponent.CoStreamTask;
import org.apache.flink.streaming.api.streamcomponent.StreamIterationSink;
import org.apache.flink.streaming.api.streamcomponent.StreamIterationSource;
import org.apache.flink.streaming.api.streamcomponent.StreamSink;
import org.apache.flink.streaming.api.streamcomponent.StreamSource;
import org.apache.flink.streaming.api.streamcomponent.StreamTask;
import org.apache.flink.streaming.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.serialization.TypeSerializerWrapper;

/**
 * Object for building Apache Flink stream processing job graphs
 */
public class JobGraphBuilder {

	private static final Log LOG = LogFactory.getLog(JobGraphBuilder.class);
	private final JobGraph jobGraph;

	// Graph attributes
	private Map<String, AbstractJobVertex> components;
	private Map<String, Integer> componentParallelism;
	private Map<String, Long> bufferTimeout;
	private Map<String, List<String>> outEdgeList;
	private Map<String, List<Integer>> outEdgeType;
	private Map<String, List<List<String>>> outEdgeNames;
	private Map<String, Boolean> mutability;
	private Map<String, List<String>> inEdgeList;
	private Map<String, List<StreamPartitioner<?>>> connectionTypes;
	private Map<String, String> operatorNames;
	private Map<String, StreamComponentInvokable<?>> invokableObjects;
	private Map<String, TypeSerializerWrapper<?, ?, ?>> typeWrappers;
	private Map<String, byte[]> serializedFunctions;
	private Map<String, byte[]> outputSelectors;
	private Map<String, Class<? extends AbstractInvokable>> componentClasses;
	private Map<String, String> iterationIds;
	private Map<String, String> iterationIDtoSourceName;
	private Map<String, String> iterationIDtoSinkName;
	private Map<String, Integer> iterationTailCount;
	private Map<String, Long> iterationWaitTime;

	private int degreeOfParallelism;
	private int executionParallelism;

	private String maxParallelismVertexName;
	private int maxParallelism;

	/**
	 * Creates an new {@link JobGraph} with the given name. A JobGraph is a DAG
	 * and consists of sources, tasks (intermediate vertices) and sinks. A
	 * JobGraph must contain at least a source and a sink.
	 * 
	 * @param jobGraphName
	 *            Name of the JobGraph
	 */
	public JobGraphBuilder(String jobGraphName) {

		jobGraph = new JobGraph(jobGraphName);

		components = new HashMap<String, AbstractJobVertex>();
		componentParallelism = new HashMap<String, Integer>();
		bufferTimeout = new HashMap<String, Long>();
		outEdgeList = new HashMap<String, List<String>>();
		outEdgeType = new HashMap<String, List<Integer>>();
		outEdgeNames = new HashMap<String, List<List<String>>>();
		mutability = new HashMap<String, Boolean>();
		inEdgeList = new HashMap<String, List<String>>();
		connectionTypes = new HashMap<String, List<StreamPartitioner<?>>>();
		operatorNames = new HashMap<String, String>();
		invokableObjects = new HashMap<String, StreamComponentInvokable<?>>();
		typeWrappers = new HashMap<String, TypeSerializerWrapper<?, ?, ?>>();
		serializedFunctions = new HashMap<String, byte[]>();
		outputSelectors = new HashMap<String, byte[]>();
		componentClasses = new HashMap<String, Class<? extends AbstractInvokable>>();
		iterationIds = new HashMap<String, String>();
		iterationIDtoSourceName = new HashMap<String, String>();
		iterationIDtoSinkName = new HashMap<String, String>();
		iterationTailCount = new HashMap<String, Integer>();
		iterationWaitTime = new HashMap<String, Long>();

		maxParallelismVertexName = "";
		maxParallelism = 0;
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
	 * Adds source to the JobGraph with the given parameters
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param InvokableObject
	 *            User defined operator
	 * @param operatorName
	 *            Operator type
	 * @param serializedFunction
	 *            Serialized udf
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public void addSource(String componentName, SourceInvokable<?> InvokableObject,
			TypeSerializerWrapper<?, ?, ?> typeWrapper, String operatorName,
			byte[] serializedFunction, int parallelism) {

		addComponent(componentName, StreamSource.class, typeWrapper, InvokableObject, operatorName,
				serializedFunction, parallelism);

		if (LOG.isDebugEnabled()) {
			LOG.debug("SOURCE: " + componentName);
		}
	}

	/**
	 * Adds a source to the iteration head to the {@link JobGraph}. The iterated
	 * tuples will be fed from this component back to the graph.
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param iterationHead
	 *            Id of the iteration head
	 * @param iterationID
	 *            ID of iteration for multiple iterations
	 * @param parallelism
	 *            Number of parallel instances created
	 * @param waitTime
	 *            Max wait time for next record
	 */
	public void addIterationSource(String componentName, String iterationHead, String iterationID,
			int parallelism, long waitTime) {

		addComponent(componentName, StreamIterationSource.class, null, null, null, null,
				parallelism);
		iterationIds.put(componentName, iterationID);
		iterationIDtoSourceName.put(iterationID, componentName);

		setBytesFrom(iterationHead, componentName);

		setEdge(componentName, iterationHead,
				connectionTypes.get(inEdgeList.get(iterationHead).get(0)).get(0), 0,
				new ArrayList<String>());

		iterationWaitTime.put(iterationIDtoSourceName.get(iterationID), waitTime);

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SOURCE: " + componentName);
		}
	}

	/**
	 * Adds a task to the JobGraph with the given parameters
	 * 
	 * @param componentNameTypeSerializerWrapper
	 *            <?, ?, ?> typeWrapper, Name of the component
	 * @param taskInvokableObject
	 *            User defined operator
	 * @param operatorName
	 *            Operator type
	 * @param serializedFunction
	 *            Serialized udf
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public <IN, OUT> void addTask(String componentName,
			UserTaskInvokable<IN, OUT> taskInvokableObject,
			TypeSerializerWrapper<?, ?, ?> typeWrapper, String operatorName,
			byte[] serializedFunction, int parallelism) {

		addComponent(componentName, StreamTask.class, typeWrapper, taskInvokableObject,
				operatorName, serializedFunction, parallelism);

		if (LOG.isDebugEnabled()) {
			LOG.debug("TASK: " + componentName);
		}
	}

	public <IN1, IN2, OUT> void addCoTask(String componentName,
			CoInvokable<IN1, IN2, OUT> taskInvokableObject,
			TypeSerializerWrapper<?, ?, ?> typeWrapper, String operatorName,
			byte[] serializedFunction, int parallelism) {

		addComponent(componentName, CoStreamTask.class, typeWrapper, taskInvokableObject,
				operatorName, serializedFunction, parallelism);

		if (LOG.isDebugEnabled()) {
			LOG.debug("CO-TASK: " + componentName);
		}
	}

	/**
	 * Adds sink to the JobGraph with the given parameters
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param InvokableObject
	 *            User defined operator
	 * @param operatorName
	 *            Operator type
	 * @param serializedFunction
	 *            Serialized udf
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public void addSink(String componentName, SinkInvokable<?> InvokableObject,
			TypeSerializerWrapper<?, ?, ?> typeWrapper, String operatorName,
			byte[] serializedFunction, int parallelism) {

		addComponent(componentName, StreamSink.class, typeWrapper, InvokableObject, operatorName,
				serializedFunction, parallelism);

		if (LOG.isDebugEnabled()) {
			LOG.debug("SINK: " + componentName);
		}

	}

	/**
	 * Adds a sink to an iteration tail to the {@link JobGraph}. The tuples
	 * intended to be iterated will be sent to this sink from the iteration
	 * head.
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param iterationTail
	 *            Id of the iteration tail
	 * @param iterationID
	 *            ID of iteration for mulitple iterations
	 * @param parallelism
	 *            Number of parallel instances created
	 * @param directName
	 *            Id of the output direction
	 * @param waitTime
	 *            Max waiting time for next record
	 */
	public void addIterationSink(String componentName, String iterationTail, String iterationID,
			int parallelism, long waitTime) {

		addComponent(componentName, StreamIterationSink.class, null, null, null, null, parallelism);
		iterationIds.put(componentName, iterationID);
		iterationIDtoSinkName.put(iterationID, componentName);
		setBytesFrom(iterationTail, componentName);
		iterationWaitTime.put(iterationIDtoSinkName.get(iterationID), waitTime);

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SINK: " + componentName);
		}

	}

	/**
	 * Sets component parameters in the JobGraph
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param componentClass
	 *            The class of the vertex
	 * @param typeWrapper
	 *            Wrapper of the types for serialization
	 * @param invokableObject
	 *            The user defined invokable object
	 * @param operatorName
	 *            Type of the user defined operator
	 * @param serializedFunction
	 *            Serialized operator
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	private void addComponent(String componentName,
			Class<? extends AbstractInvokable> componentClass,
			TypeSerializerWrapper<?, ?, ?> typeWrapper,
			StreamComponentInvokable<?> invokableObject, String operatorName,
			byte[] serializedFunction, int parallelism) {

		componentClasses.put(componentName, componentClass);
		typeWrappers.put(componentName, typeWrapper);
		setParallelism(componentName, parallelism);
		mutability.put(componentName, false);
		invokableObjects.put(componentName, invokableObject);
		operatorNames.put(componentName, operatorName);
		serializedFunctions.put(componentName, serializedFunction);
		outEdgeList.put(componentName, new ArrayList<String>());
		outEdgeType.put(componentName, new ArrayList<Integer>());
		outEdgeNames.put(componentName, new ArrayList<List<String>>());
		inEdgeList.put(componentName, new ArrayList<String>());
		connectionTypes.put(componentName, new ArrayList<StreamPartitioner<?>>());
		iterationTailCount.put(componentName, 0);
	}

	/**
	 * Creates an {@link AbstractJobVertex} in the {@link JobGraph} and sets its
	 * config parameters using the ones set previously.
	 * 
	 * @param componentName
	 *            Name of the component for which the vertex will be created.
	 */
	private void createVertex(String componentName) {

		// Get vertex attributes
		Class<? extends AbstractInvokable> componentClass = componentClasses.get(componentName);
		StreamComponentInvokable<?> invokableObject = invokableObjects.get(componentName);
		String operatorName = operatorNames.get(componentName);
		byte[] serializedFunction = serializedFunctions.get(componentName);
		int parallelism = componentParallelism.get(componentName);
		byte[] outputSelector = outputSelectors.get(componentName);

		// Create vertex object
		AbstractJobVertex component = null;
		if (componentClass.equals(StreamSource.class)
				|| componentClass.equals(StreamIterationSource.class)) {
			component = new JobInputVertex(componentName, this.jobGraph);
		} else if (componentClass.equals(StreamTask.class)
				|| componentClass.equals(CoStreamTask.class)) {
			component = new JobTaskVertex(componentName, this.jobGraph);
		} else if (componentClass.equals(StreamSink.class)
				|| componentClass.equals(StreamIterationSink.class)) {
			component = new JobOutputVertex(componentName, this.jobGraph);
		} else {
			throw new RuntimeException("Unsupported component class");
		}

		component.setInvokableClass(componentClass);
		component.setNumberOfSubtasks(parallelism);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: " + parallelism + " for " + componentName);
		}

		StreamConfig config = new StreamConfig(component.getConfiguration());

		config.setMutability(mutability.get(componentName));
		config.setBufferTimeout(bufferTimeout.get(componentName));
		config.setTypeWrapper(typeWrappers.get(componentName));
		// Set vertex config
		config.setUserInvokable(invokableObject);
		config.setComponentName(componentName);
		config.setFunction(serializedFunction, operatorName);
		config.setOutputSelector(outputSelector);

		if (componentClass.equals(StreamIterationSource.class)
				|| componentClass.equals(StreamIterationSink.class)) {
			config.setIterationId(iterationIds.get(componentName));
			config.setIterationWaitTime(iterationWaitTime.get(componentName));
		}

		components.put(componentName, component);

		if (parallelism > maxParallelism) {
			maxParallelism = parallelism;
			maxParallelismVertexName = componentName;
		}
	}

	/**
	 * Sets the number of parallel instances created for the given component.
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public void setParallelism(String componentName, int parallelism) {
		componentParallelism.put(componentName, parallelism);
	}

	public void setMutability(String componentName, boolean isMutable) {
		mutability.put(componentName, isMutable);
	}

	public void setBufferTimeout(String componentName, long bufferTimeout) {
		this.bufferTimeout.put(componentName, bufferTimeout);
	}

	/**
	 * Connects two vertices in the JobGraph using the selected partitioner
	 * settings
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream(output) vertex
	 * @param downStreamComponentName
	 *            Name of the downstream(input) vertex
	 * @param partitionerObject
	 *            Partitioner object
	 * @param typeNumber
	 *            Number of the type (used at co-functions)
	 * @param outputNames
	 *            User defined names of the out edge
	 */
	public void setEdge(String upStreamComponentName, String downStreamComponentName,
			StreamPartitioner<?> partitionerObject, int typeNumber, List<String> outputNames) {
		outEdgeList.get(upStreamComponentName).add(downStreamComponentName);
		outEdgeType.get(upStreamComponentName).add(typeNumber);
		inEdgeList.get(downStreamComponentName).add(upStreamComponentName);
		connectionTypes.get(upStreamComponentName).add(partitionerObject);
		outEdgeNames.get(upStreamComponentName).add(outputNames);
	}

	/**
	 * Connects to JobGraph components with the given names, partitioning and
	 * channel type
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 * @param partitionerObject
	 *            The partitioner
	 */
	private <T> void connect(String upStreamComponentName, String downStreamComponentName,
			StreamPartitioner<T> partitionerObject) {

		AbstractJobVertex upStreamComponent = components.get(upStreamComponentName);
		AbstractJobVertex downStreamComponent = components.get(downStreamComponentName);

		StreamConfig config = new StreamConfig(upStreamComponent.getConfiguration());

		try {
			if (partitionerObject.getClass().equals(ForwardPartitioner.class)) {
				upStreamComponent.connectTo(downStreamComponent, ChannelType.NETWORK,
						DistributionPattern.POINTWISE);
			} else {
				upStreamComponent.connectTo(downStreamComponent, ChannelType.NETWORK,
						DistributionPattern.BIPARTITE);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("CONNECTED: " + partitionerObject.getClass().getSimpleName() + " - "
						+ upStreamComponentName + " -> " + downStreamComponentName);
			}

		} catch (JobGraphDefinitionException e) {
			throw new RuntimeException("Cannot connect components: " + upStreamComponentName
					+ " to " + downStreamComponentName, e);
		}

		int outputIndex = upStreamComponent.getNumberOfForwardConnections() - 1;

		config.setOutputName(outputIndex, outEdgeNames.get(upStreamComponentName).get(outputIndex));
		config.setPartitioner(outputIndex, partitionerObject);
		config.setNumberOfOutputChannels(outputIndex,
				componentParallelism.get(downStreamComponentName));
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
		setParallelism(iterationIDtoSourceName.get(iterationID),
				componentParallelism.get(iterationTail));
		setBufferTimeout(iterationIDtoSourceName.get(iterationID), bufferTimeout.get(iterationTail));
	}

	/**
	 * Sets a user defined {@link OutputSelector} for the given component. Used
	 * for directed emits.
	 * 
	 * @param componentName
	 *            Name of the component for which the output selector will be
	 *            set
	 * @param serializedOutputSelector
	 *            Byte array representing the serialized output selector.
	 */
	public <T> void setOutputSelector(String componentName, byte[] serializedOutputSelector) {
		outputSelectors.put(componentName, serializedOutputSelector);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Outputselector set for " + componentName);
		}

	}

	/**
	 * Sets udf operator and TypeSerializerWrapper from one component to
	 * another, used with some sinks.
	 * 
	 * @param from
	 *            from
	 * @param to
	 *            to
	 */
	public void setBytesFrom(String from, String to) {

		operatorNames.put(to, operatorNames.get(from));
		serializedFunctions.put(to, serializedFunctions.get(from));
		typeWrappers.put(to, typeWrappers.get(from));
	}

	/**
	 * Sets instance sharing between the given components
	 * 
	 * @param component1
	 *            Share will be called on this component
	 * @param component2
	 *            Share will be called to this component
	 */
	public void setInstanceSharing(String component1, String component2) {
		AbstractJobVertex c1 = components.get(component1);
		AbstractJobVertex c2 = components.get(component2);

		c1.setVertexToShareInstancesWith(c2);
	}

	/**
	 * Sets all components to share with the one with highest parallelism
	 */
	private void setAutomaticInstanceSharing() {

		AbstractJobVertex maxParallelismVertex = components.get(maxParallelismVertexName);

		for (String componentName : components.keySet()) {
			if (!componentName.equals(maxParallelismVertexName)) {
				components.get(componentName).setVertexToShareInstancesWith(maxParallelismVertex);
			}
		}

	}

	/**
	 * Writes number of inputs into each JobVertex's config
	 */
	private void setNumberOfJobInputs() {
		for (AbstractJobVertex component : components.values()) {
			(new StreamConfig(component.getConfiguration())).setNumberOfInputs(component
					.getNumberOfBackwardConnections());
		}
	}

	/**
	 * Writes the number of outputs and output channels into each JobVertex's
	 * config
	 */
	private void setNumberOfJobOutputs() {
		for (AbstractJobVertex component : components.values()) {
			(new StreamConfig(component.getConfiguration())).setNumberOfOutputs(component
					.getNumberOfForwardConnections());
		}
	}

	/**
	 * Builds the {@link JobGraph} from the components with the edges and
	 * settings provided.
	 */
	private void buildGraph() {

		for (String componentName : outEdgeList.keySet()) {
			createVertex(componentName);
		}

		for (String upStreamComponentName : outEdgeList.keySet()) {
			int i = 0;

			List<Integer> outEdgeTypeList = outEdgeType.get(upStreamComponentName);

			for (String downStreamComponentName : outEdgeList.get(upStreamComponentName)) {
				StreamConfig downStreamComponentConfig = new StreamConfig(components.get(
						downStreamComponentName).getConfiguration());

				int inputNumber = downStreamComponentConfig.getNumberOfInputs();

				downStreamComponentConfig.setInputType(inputNumber++, outEdgeTypeList.get(i));
				downStreamComponentConfig.setNumberOfInputs(inputNumber);

				connect(upStreamComponentName, downStreamComponentName,
						connectionTypes.get(upStreamComponentName).get(i));
				i++;
			}
		}

		setAutomaticInstanceSharing();
		setNumberOfJobInputs();
		setNumberOfJobOutputs();
	}

	/**
	 * Builds and returns the JobGraph
	 * 
	 * @return JobGraph object
	 */
	public JobGraph getJobGraph() {
		buildGraph();
		return jobGraph;
	}

}
