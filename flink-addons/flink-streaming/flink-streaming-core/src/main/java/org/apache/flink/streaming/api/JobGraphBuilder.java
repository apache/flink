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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.streaming.api.invokable.StreamComponentInvokable;
import org.apache.flink.streaming.api.invokable.UserSinkInvokable;
import org.apache.flink.streaming.api.invokable.UserSourceInvokable;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamcomponent.CoStreamTask;
import org.apache.flink.streaming.api.streamcomponent.StreamIterationSink;
import org.apache.flink.streaming.api.streamcomponent.StreamIterationSource;
import org.apache.flink.streaming.api.streamcomponent.StreamSink;
import org.apache.flink.streaming.api.streamcomponent.StreamSource;
import org.apache.flink.streaming.api.streamcomponent.StreamTask;
import org.apache.flink.streaming.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.partitioner.DistributePartitioner;
import org.apache.flink.streaming.partitioner.FieldsPartitioner;
import org.apache.flink.streaming.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.partitioner.StreamPartitioner;

/**
 * Object for building Flink stream processing job graphs
 */
public class JobGraphBuilder {

	private static final Log LOG = LogFactory.getLog(JobGraphBuilder.class);
	private final JobGraph jobGraph;

	// Graph attributes
	private Map<String, AbstractJobVertex> components;
	private Map<String, Integer> componentParallelism;
	private Map<String, ArrayList<String>> outEdgeList;
	private Map<String, ArrayList<Integer>> outEdgeType;
	private Map<String, List<String>> inEdgeList;
	private Map<String, List<StreamPartitioner<? extends Tuple>>> connectionTypes;
	private Map<String, String> userDefinedNames;
	private Map<String, String> operatorNames;
	private Map<String, StreamComponentInvokable> invokableObjects;
	private Map<String, byte[]> serializedFunctions;
	private Map<String, byte[]> outputSelectors;
	private Map<String, Class<? extends AbstractInvokable>> componentClasses;
	private Map<String, String> iterationIds;
	private Map<String, String> iterationHeadNames;
	private Map<String, Integer> iterationTailCount;

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
		outEdgeList = new HashMap<String, ArrayList<String>>();
		outEdgeType = new HashMap<String, ArrayList<Integer>>();
		inEdgeList = new HashMap<String, List<String>>();
		connectionTypes = new HashMap<String, List<StreamPartitioner<? extends Tuple>>>();
		userDefinedNames = new HashMap<String, String>();
		operatorNames = new HashMap<String, String>();
		invokableObjects = new HashMap<String, StreamComponentInvokable>();
		serializedFunctions = new HashMap<String, byte[]>();
		outputSelectors = new HashMap<String, byte[]>();
		componentClasses = new HashMap<String, Class<? extends AbstractInvokable>>();
		iterationIds = new HashMap<String, String>();
		iterationHeadNames = new HashMap<String, String>();
		iterationTailCount = new HashMap<String, Integer>();

		maxParallelismVertexName = "";
		maxParallelism = 0;
		if (LOG.isDebugEnabled()) {
			LOG.debug("JobGraph created");
		}
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
	public void addSource(String componentName,
			UserSourceInvokable<? extends Tuple> InvokableObject, String operatorName,
			byte[] serializedFunction, int parallelism) {

		addComponent(componentName, StreamSource.class, InvokableObject, operatorName,
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
	 */
	public void addIterationSource(String componentName, String iterationHead, String iterationID,
			int parallelism) {

		addComponent(componentName, StreamIterationSource.class, null, null, null, parallelism);
		iterationIds.put(componentName, iterationID);
		iterationHeadNames.put(iterationID, componentName);

		setBytesFrom(iterationHead, componentName);

		setEdge(componentName, iterationHead,
				connectionTypes.get(inEdgeList.get(iterationHead).get(0)).get(0), 0);

		if (LOG.isDebugEnabled()) {
			LOG.debug("ITERATION SOURCE: " + componentName);
		}
	}

	/**
	 * Adds a task to the JobGraph with the given parameters
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param taskInvokableObject
	 *            User defined operator
	 * @param operatorName
	 *            Operator type
	 * @param serializedFunction
	 *            Serialized udf
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public <IN extends Tuple, OUT extends Tuple> void addTask(String componentName,
			UserTaskInvokable<IN, OUT> taskInvokableObject, String operatorName,
			byte[] serializedFunction, int parallelism) {

		addComponent(componentName, StreamTask.class, taskInvokableObject, operatorName,
				serializedFunction, parallelism);

		if (LOG.isDebugEnabled()) {
			LOG.debug("TASK: " + componentName);
		}
	}

	public <IN1 extends Tuple, IN2 extends Tuple, OUT extends Tuple> void addCoTask(
			String componentName, CoInvokable<IN1, IN2, OUT> taskInvokableObject,
			String operatorName, byte[] serializedFunction, int parallelism) {

		addComponent(componentName, CoStreamTask.class, taskInvokableObject, operatorName,
				serializedFunction, parallelism);

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
	public void addSink(String componentName, UserSinkInvokable<? extends Tuple> InvokableObject,
			String operatorName, byte[] serializedFunction, int parallelism) {

		addComponent(componentName, StreamSink.class, InvokableObject, operatorName,
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
	 */
	public void addIterationSink(String componentName, String iterationTail, String iterationID,
			int parallelism, String directName) {

		addComponent(componentName, StreamIterationSink.class, null, null, null, parallelism);
		iterationIds.put(componentName, iterationID);
		setBytesFrom(iterationTail, componentName);

		if (directName != null) {
			setUserDefinedName(componentName, directName);
		} else {
			setUserDefinedName(componentName, "iterate");
		}

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
			StreamComponentInvokable invokableObject, String operatorName,
			byte[] serializedFunction, int parallelism) {

		componentClasses.put(componentName, componentClass);
		setParallelism(componentName, parallelism);
		invokableObjects.put(componentName, invokableObject);
		operatorNames.put(componentName, operatorName);
		serializedFunctions.put(componentName, serializedFunction);
		outEdgeList.put(componentName, new ArrayList<String>());
		outEdgeType.put(componentName, new ArrayList<Integer>());
		inEdgeList.put(componentName, new ArrayList<String>());
		connectionTypes.put(componentName, new ArrayList<StreamPartitioner<? extends Tuple>>());
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
		StreamComponentInvokable invokableObject = invokableObjects.get(componentName);
		String operatorName = operatorNames.get(componentName);
		byte[] serializedFunction = serializedFunctions.get(componentName);
		int parallelism = componentParallelism.get(componentName);
		byte[] outputSelector = outputSelectors.get(componentName);
		String userDefinedName = userDefinedNames.get(componentName);

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
		}

		component.setInvokableClass(componentClass);
		component.setNumberOfSubtasks(parallelism);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: " + parallelism + " for " + componentName);
		}

		Configuration config = component.getConfiguration();

		// Set vertex config
		if (invokableObject != null) {
			config.setClass("userfunction", invokableObject.getClass());
			addSerializedObject(invokableObject, config);
		}
		config.setString("componentName", componentName);
		if (serializedFunction != null) {
			config.setBytes("operator", serializedFunction);
			config.setString("operatorName", operatorName);
		}

		if (userDefinedName != null) {
			config.setString("userDefinedName", userDefinedName);
		}

		if (outputSelector != null) {
			config.setBoolean("directedEmit", true);
			config.setBytes("outputSelector", outputSelector);
		}

		if (componentClass.equals(StreamIterationSource.class)
				|| componentClass.equals(StreamIterationSink.class)) {
			config.setString("iteration-id", iterationIds.get(componentName));
		}

		components.put(componentName, component);

		if (parallelism > maxParallelism) {
			maxParallelism = parallelism;
			maxParallelismVertexName = componentName;
		}
	}

	/**
	 * Adds serialized invokable object to the JobVertex configuration
	 * 
	 * @param invokableObject
	 *            Invokable object to serialize
	 * @param config
	 *            JobVertex configuration to which the serialized invokable will
	 *            be added
	 */
	private void addSerializedObject(Serializable invokableObject, Configuration config) {

		ByteArrayOutputStream baos = null;
		ObjectOutputStream oos = null;
		try {
			baos = new ByteArrayOutputStream();

			oos = new ObjectOutputStream(baos);

			oos.writeObject(invokableObject);

			config.setBytes("serializedudf", baos.toByteArray());
		} catch (Exception e) {
			throw new RuntimeException("Cannot serialize invokable object "
					+ invokableObject.getClass(), e);
		}

	}

	/**
	 * Sets the user defined name for the selected component
	 * 
	 * @param componentName
	 *            Name of the component for which the user defined name will be
	 *            set
	 * @param userDefinedName
	 *            User defined name to set for the component
	 */
	public void setUserDefinedName(String componentName, String userDefinedName) {
		userDefinedNames.put(componentName, userDefinedName);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Name set: " + userDefinedName + " for " + componentName);
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
	 */
	public void setEdge(String upStreamComponentName, String downStreamComponentName,
			StreamPartitioner<? extends Tuple> partitionerObject, int typeNumber) {
		outEdgeList.get(upStreamComponentName).add(downStreamComponentName);
		outEdgeType.get(upStreamComponentName).add(typeNumber);
		inEdgeList.get(downStreamComponentName).add(upStreamComponentName);
		connectionTypes.get(upStreamComponentName).add(partitionerObject);
	}

	/**
	 * Connects two components with the given names by broadcast partitioning.
	 * <p>
	 * Broadcast partitioning: All the emitted tuples are replicated to all of
	 * the output instances
	 * 
	 * @param inputStream
	 *            The DataStream object of the input
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the records
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the
	 *            records
	 * @param typeNumber
	 *            Number of the type (used at co-functions)
	 */
	public <T extends Tuple> void broadcastConnect(DataStream<T> inputStream,
			String upStreamComponentName, String downStreamComponentName, int typeNumber) {
		setEdge(upStreamComponentName, downStreamComponentName, new BroadcastPartitioner<T>(),
				typeNumber);
	}

	/**
	 * Connects two components with the given names by fields partitioning on
	 * the given field.
	 * <p>
	 * Fields partitioning: Tuples are hashed by the given key, and grouped to
	 * outputs accordingly
	 * 
	 * @param inputStream
	 *            The DataStream object of the input
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the records
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the
	 *            records
	 * @param keyPosition
	 *            Position of key in the tuple
	 * @param typeNumber
	 *            Number of the type (used at co-functions)
	 */
	public <T extends Tuple> void fieldsConnect(DataStream<T> inputStream,
			String upStreamComponentName, String downStreamComponentName, int keyPosition,
			int typeNumber) {

		setEdge(upStreamComponentName, downStreamComponentName, new FieldsPartitioner<T>(
				keyPosition), typeNumber);
	}

	/**
	 * Connects two components with the given names by global partitioning.
	 * <p>
	 * Global partitioning: sends all emitted tuples to one output instance
	 * (i.e. the first one)
	 * 
	 * @param inputStream
	 *            The DataStream object of the input
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 * @param typeNumber
	 *            Number of the type (used at co-functions)
	 */
	public <T extends Tuple> void globalConnect(DataStream<T> inputStream,
			String upStreamComponentName, String downStreamComponentName, int typeNumber) {
		setEdge(upStreamComponentName, downStreamComponentName, new GlobalPartitioner<T>(),
				typeNumber);
	}

	/**
	 * Connects two components with the given names by shuffle partitioning.
	 * <p>
	 * Shuffle partitioning: sends the output tuples to a randomly selected
	 * channel
	 * 
	 * @param inputStream
	 *            The DataStream object of the input
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 * @param typeNumber
	 *            Number of the type (used at co-functions)
	 */
	public <T extends Tuple> void shuffleConnect(DataStream<T> inputStream,
			String upStreamComponentName, String downStreamComponentName, int typeNumber) {
		setEdge(upStreamComponentName, downStreamComponentName, new ShufflePartitioner<T>(),
				typeNumber);
	}

	/**
	 * Connects two components with the given names by connecting the local
	 * subtasks in memory.
	 * <p>
	 * Forward partitioning: sends the output tuples to the local subtask of the
	 * output vertex
	 * 
	 * @param inputStream
	 *            The DataStream object of the input
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 * @param typeNumber
	 *            Number of the type (used at co-functions)
	 */
	public <T extends Tuple> void forwardConnect(DataStream<T> inputStream,
			String upStreamComponentName, String downStreamComponentName, int typeNumber) {
		setEdge(upStreamComponentName, downStreamComponentName, new ForwardPartitioner<T>(),
				typeNumber);
	}
	
	/**
	 * Connects two components with the given names by distribute partitioning.
	 * <p>
	 * Distribute partitioning: sends the output tuples evenly distributed
	 * along the selected channels
	 * 
	 * @param inputStream
	 *            The DataStream object of the input
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 */
	public <T extends Tuple> void distributeConnect(DataStream<T> inputStream,
			String upStreamComponentName, String downStreamComponentName) {
		setEdge(upStreamComponentName, downStreamComponentName, new DistributePartitioner<T>());
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
	private <T extends Tuple> void connect(String upStreamComponentName,
			String downStreamComponentName, StreamPartitioner<T> partitionerObject) {

		AbstractJobVertex upStreamComponent = components.get(upStreamComponentName);
		AbstractJobVertex downStreamComponent = components.get(downStreamComponentName);

		Configuration config = upStreamComponent.getConfiguration();

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

		putOutputNameToConfig(upStreamComponentName, downStreamComponentName, outputIndex, config);

		config.setBytes("partitionerObject_" + outputIndex,
				SerializationUtils.serialize(partitionerObject));

		config.setInteger("numOfOutputs_" + outputIndex,
				componentParallelism.get(downStreamComponentName));

	}

	/**
	 * Sets the user defined name for an output edge in the graph
	 * 
	 * @param upStreamComponentName
	 *            The name of the component to which the output name will be set
	 * @param downStreamComponentName
	 *            The name of the component representing the output
	 * @param index
	 *            Index of the output channel
	 * @param config
	 *            Config of the upstream component
	 */
	private void putOutputNameToConfig(String upStreamComponentName,
			String downStreamComponentName, int index, Configuration config) {

		String outputName = userDefinedNames.get(downStreamComponentName);
		if (outputName != null) {
			config.setString("outputName_" + (index), outputName);
		}
	}

	/**
	 * Sets the parallelism of the iteration head of the given iteration id to
	 * the parallelism given.
	 * 
	 * @param iterationID
	 *            ID of the iteration
	 * @param parallelism
	 *            Parallelism to set, typically the parallelism of the iteration
	 *            tail.
	 */
	public void setIterationSourceParallelism(String iterationID, int parallelism) {
		setParallelism(iterationHeadNames.get(iterationID), parallelism);
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
	public <T extends Tuple> void setOutputSelector(String componentName,
			byte[] serializedOutputSelector) {
		outputSelectors.put(componentName, serializedOutputSelector);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Outputselector set for " + componentName);
		}

	}

	/**
	 * Sets udf operator from one component to another, used with some sinks.
	 * 
	 * @param from
	 *            from
	 * @param to
	 *            to
	 */
	public void setBytesFrom(String from, String to) {

		operatorNames.put(to, operatorNames.get(from));
		serializedFunctions.put(to, serializedFunctions.get(from));

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
			if (componentName != maxParallelismVertexName) {
				components.get(componentName).setVertexToShareInstancesWith(maxParallelismVertex);
			}
		}

	}

	/**
	 * Writes number of inputs into each JobVertex's config
	 */
	private void setNumberOfJobInputs() {
		for (AbstractJobVertex component : components.values()) {
			component.getConfiguration().setInteger("numberOfInputs",
					component.getNumberOfBackwardConnections());
		}
	}

	/**
	 * Writes the number of outputs and output channels into each JobVertex's
	 * config
	 */
	private void setNumberOfJobOutputs() {
		for (AbstractJobVertex component : components.values()) {
			component.getConfiguration().setInteger("numberOfOutputs",
					component.getNumberOfForwardConnections());
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
			
			ArrayList<Integer> outEdgeTypeList = outEdgeType.get(upStreamComponentName);

			for (String downStreamComponentName : outEdgeList.get(upStreamComponentName)) {
				Configuration downStreamComponentConfig = components.get(downStreamComponentName)
						.getConfiguration();
				
				int inputNumber = downStreamComponentConfig.getInteger("numberOfInputs", 0);				
				downStreamComponentConfig.setInteger("inputType_" + inputNumber++, outEdgeTypeList.get(i));
				downStreamComponentConfig.setInteger("numberOfInputs", inputNumber);
				
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
