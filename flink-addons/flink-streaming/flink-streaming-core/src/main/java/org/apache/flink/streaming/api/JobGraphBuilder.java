/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package org.apache.flink.streaming.api;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.io.network.channels.ChannelType;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphDefinitionException;
import org.apache.flink.runtime.jobgraph.JobInputVertex;
import org.apache.flink.runtime.jobgraph.JobOutputVertex;
import org.apache.flink.runtime.jobgraph.JobTaskVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.streaming.api.invokable.StreamComponentInvokable;
import org.apache.flink.streaming.api.invokable.UserSinkInvokable;
import org.apache.flink.streaming.api.invokable.UserSourceInvokable;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.streamcomponent.StreamIterationSink;
import org.apache.flink.streaming.api.streamcomponent.StreamIterationSource;
import org.apache.flink.streaming.api.streamcomponent.StreamSink;
import org.apache.flink.streaming.api.streamcomponent.StreamSource;
import org.apache.flink.streaming.api.streamcomponent.StreamTask;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.faulttolerance.FaultToleranceType;
import org.apache.flink.streaming.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.partitioner.FieldsPartitioner;
import org.apache.flink.streaming.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.partitioner.ShufflePartitioner;

/**
 * Object for building Flink stream processing job graphs
 */
public class JobGraphBuilder {

	private static final Log log = LogFactory.getLog(JobGraphBuilder.class);
	private final JobGraph jobGraph;

	// Graph attributes
	private Map<String, AbstractJobVertex> components;
	private Map<String, Integer> componentParallelism;
	private Map<String, List<String>> edgeList;
	private Map<String, List<Class<? extends ChannelSelector<StreamRecord>>>> connectionTypes;
	private Map<String, List<Integer>> connectionParams;
	private Map<String, String> userDefinedNames;
	private Map<String, String> operatorNames;
	private Map<String, StreamComponentInvokable> invokableObjects;
	private Map<String, byte[]> serializedFunctions;
	private Map<String, byte[]> outputSelectors;
	private Map<String, Class<? extends AbstractInvokable>> componentClasses;
	private Map<String, List<Integer>> batchSizes;

	private String maxParallelismVertexName;
	private int maxParallelism;
	private FaultToleranceType faultToleranceType;
	private long batchTimeout = 1000;

	/**
	 * Creates a new JobGraph with the given name
	 * 
	 * @param jobGraphName
	 *            Name of the JobGraph
	 * @param faultToleranceType
	 *            Fault tolerance type
	 */
	public JobGraphBuilder(String jobGraphName, FaultToleranceType faultToleranceType) {

		jobGraph = new JobGraph(jobGraphName);

		components = new HashMap<String, AbstractJobVertex>();
		componentParallelism = new HashMap<String, Integer>();
		edgeList = new HashMap<String, List<String>>();
		connectionTypes = new HashMap<String, List<Class<? extends ChannelSelector<StreamRecord>>>>();
		connectionParams = new HashMap<String, List<Integer>>();
		userDefinedNames = new HashMap<String, String>();
		operatorNames = new HashMap<String, String>();
		invokableObjects = new HashMap<String, StreamComponentInvokable>();
		serializedFunctions = new HashMap<String, byte[]>();
		outputSelectors = new HashMap<String, byte[]>();
		componentClasses = new HashMap<String, Class<? extends AbstractInvokable>>();
		batchSizes = new HashMap<String, List<Integer>>();

		maxParallelismVertexName = "";
		maxParallelism = 0;
		if (log.isDebugEnabled()) {
			log.debug("JobGraph created");
		}
		this.faultToleranceType = faultToleranceType;
	}

	public void setBatchTimeout(int timeout) {
		this.batchTimeout = timeout;
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
	public void setSource(String componentName,
			UserSourceInvokable<? extends Tuple> InvokableObject, String operatorName,
			byte[] serializedFunction, int parallelism) {

		setComponent(componentName, StreamSource.class, InvokableObject, operatorName,
				serializedFunction, parallelism);

		if (log.isDebugEnabled()) {
			log.debug("SOURCE: " + componentName);
		}
	}

	public void setIterationSource(String componentName, String iterationHead, int parallelism) {

		setComponent(componentName, StreamIterationSource.class, null, null, null, parallelism);

		setBytesFrom(iterationHead, componentName);

		if (log.isDebugEnabled()) {
			log.debug("Iteration head source: " + componentName);
		}
		setBatchSize(componentName, 1);
	}

	/**
	 * Adds task to the JobGraph with the given parameters
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param TaskInvokableObject
	 *            User defined operator
	 * @param operatorName
	 *            Operator type
	 * @param serializedFunction
	 *            Serialized udf
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public void setTask(String componentName,
			UserTaskInvokable<? extends Tuple, ? extends Tuple> TaskInvokableObject,
			String operatorName, byte[] serializedFunction, int parallelism) {

		setComponent(componentName, StreamTask.class, TaskInvokableObject, operatorName,
				serializedFunction, parallelism);

		if (log.isDebugEnabled()) {
			log.debug("TASK: " + componentName);
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
	public void setSink(String componentName, UserSinkInvokable<? extends Tuple> InvokableObject,
			String operatorName, byte[] serializedFunction, int parallelism) {

		setComponent(componentName, StreamSink.class, InvokableObject, operatorName,
				serializedFunction, parallelism);

		if (log.isDebugEnabled()) {
			log.debug("SINK: " + componentName);
		}

	}

	public void setIterationSink(String componentName, String iterationTail, int parallelism) {

		setComponent(componentName, StreamIterationSink.class, null, null, null, parallelism);

		setBytesFrom(iterationTail, componentName);
		
		setUserDefinedName(componentName, "iterate");

		if (log.isDebugEnabled()) {
			log.debug("Iteration tail sink: " + componentName);
		}

	}

	/**
	 * Sets component parameters in the JobGraph
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param componentClass
	 *            The class of the vertex
	 * @param InvokableObject
	 *            The user defined invokable object
	 * @param operatorName
	 *            Type of the user defined operator
	 * @param serializedFunction
	 *            Serialized operator
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	private void setComponent(String componentName,
			Class<? extends AbstractInvokable> componentClass,
			StreamComponentInvokable InvokableObject, String operatorName,
			byte[] serializedFunction, int parallelism) {

		componentClasses.put(componentName, componentClass);
		componentParallelism.put(componentName, parallelism);
		invokableObjects.put(componentName, InvokableObject);
		operatorNames.put(componentName, operatorName);
		serializedFunctions.put(componentName, serializedFunction);
		batchSizes.put(componentName, new ArrayList<Integer>());
		edgeList.put(componentName, new ArrayList<String>());
		connectionTypes.put(componentName,
				new ArrayList<Class<? extends ChannelSelector<StreamRecord>>>());
		connectionParams.put(componentName, new ArrayList<Integer>());
	}

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
		} else if (componentClass.equals(StreamTask.class)) {
			component = new JobTaskVertex(componentName, this.jobGraph);
		} else if (componentClass.equals(StreamSink.class)
				|| componentClass.equals(StreamIterationSink.class)) {
			component = new JobOutputVertex(componentName, this.jobGraph);
		}

		component.setInvokableClass(componentClass);
		component.setNumberOfSubtasks(parallelism);

		Configuration config = new TaskConfig(component.getConfiguration()).getConfiguration();

		// Set vertex config
		if (invokableObject != null) {
			config.setClass("userfunction", invokableObject.getClass());
			addSerializedObject(invokableObject, config);
		}
		config.setString("componentName", componentName);
		config.setLong("batchTimeout", this.batchTimeout);
		config.setInteger("faultToleranceType", this.faultToleranceType.id);
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
			config.setString("iteration-id", "iteration-0");
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
	 * @param InvokableObject
	 *            Invokable object to serialize
	 * @param config
	 *            JobVertex configuration to which the serialized invokable will
	 *            be added
	 */
	private void addSerializedObject(Serializable InvokableObject, Configuration config) {

		ByteArrayOutputStream baos = null;
		ObjectOutputStream oos = null;
		try {
			baos = new ByteArrayOutputStream();

			oos = new ObjectOutputStream(baos);

			oos.writeObject(InvokableObject);

			config.setBytes("serializedudf", baos.toByteArray());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Serialization error " + InvokableObject.getClass());
		}

	}

	/**
	 * Sets the number of tuples batched together for higher throughput
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param batchSize
	 *            Number of tuples batched together
	 */
	public void setBatchSize(String componentName, int batchSize) {
		batchSizes.get(componentName).add(batchSize);
	}

	public void setUserDefinedName(String componentName, String userDefinedName) {
		userDefinedNames.put(componentName, userDefinedName);
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

		if (parallelism > maxParallelism) {
			maxParallelism = parallelism;
			maxParallelismVertexName = componentName;
		}
	}

	public void setEdge(String upStreamComponentName, String downStreamComponentName,
			Class<? extends ChannelSelector<StreamRecord>> partitionerClass, int partitionerParam) {
		edgeList.get(upStreamComponentName).add(downStreamComponentName);
		connectionTypes.get(upStreamComponentName).add(partitionerClass);
		connectionParams.get(upStreamComponentName).add(partitionerParam);
	}

	/**
	 * Connects two components with the given names by broadcast partitioning.
	 * <p>
	 * Broadcast partitioning: All the emitted tuples are replicated to all of
	 * the output instances
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the records
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the
	 *            records
	 */
	public void broadcastConnect(String upStreamComponentName, String downStreamComponentName) {
		setEdge(upStreamComponentName, downStreamComponentName, BroadcastPartitioner.class, 0);
		log.info("Broadcastconnected: " + upStreamComponentName + " to " + downStreamComponentName);
	}

	/**
	 * Connects two components with the given names by fields partitioning on
	 * the given field.
	 * <p>
	 * Fields partitioning: Tuples are hashed by the given key, and grouped to
	 * outputs accordingly
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the records
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the
	 *            records
	 * @param keyPosition
	 *            Position of key in the tuple
	 */
	public void fieldsConnect(String upStreamComponentName, String downStreamComponentName,
			int keyPosition) {

		setEdge(upStreamComponentName, downStreamComponentName, FieldsPartitioner.class,
				keyPosition);

	}

	/**
	 * Connects two components with the given names by global partitioning.
	 * <p>
	 * Global partitioning: sends all emitted tuples to one output instance
	 * (i.e. the first one)
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 */
	public void globalConnect(String upStreamComponentName, String downStreamComponentName) {
		setEdge(upStreamComponentName, downStreamComponentName, GlobalPartitioner.class, 0);
		log.info("Globalconnected: " + upStreamComponentName + " to " + downStreamComponentName);

	}

	/**
	 * Connects two components with the given names by shuffle partitioning.
	 * <p>
	 * Shuffle partitioning: sends the output tuples to a randomly selected
	 * channel
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 */
	public void shuffleConnect(String upStreamComponentName, String downStreamComponentName) {
		setEdge(upStreamComponentName, downStreamComponentName, ShufflePartitioner.class, 0);
		log.info("Shuffleconnected: " + upStreamComponentName + " to " + downStreamComponentName);
	}

	/**
	 * Connects to JobGraph components with the given names, partitioning and
	 * channel type
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 * @param PartitionerClass
	 *            Class of the partitioner
	 * @param partitionerParam
	 *            Parameter of the partitioner
	 */
	private void connect(String upStreamComponentName, String downStreamComponentName,
			Class<? extends ChannelSelector<StreamRecord>> PartitionerClass, int partitionerParam) {

		AbstractJobVertex upStreamComponent = components.get(upStreamComponentName);
		AbstractJobVertex downStreamComponent = components.get(downStreamComponentName);

		Configuration config = new TaskConfig(upStreamComponent.getConfiguration())
				.getConfiguration();

		try {
			upStreamComponent.connectTo(downStreamComponent, ChannelType.NETWORK);

			if (log.isDebugEnabled()) {
				log.debug("CONNECTED: " + PartitionerClass.getSimpleName() + " - "
						+ upStreamComponentName + " -> " + downStreamComponentName);
			}

		} catch (JobGraphDefinitionException e1) {
			if (log.isErrorEnabled()) {
				log.error("Cannot connect components by field: " + upStreamComponentName + " to "
						+ downStreamComponentName, e1);
			}
		}

		int outputIndex = upStreamComponent.getNumberOfForwardConnections() - 1;

		if (PartitionerClass.equals(FieldsPartitioner.class)) {
			config.setBoolean("isPartitionedOutput_" + outputIndex, true);
		}

		putOutputNameToConfig(upStreamComponentName, downStreamComponentName, outputIndex, config);

		config.setClass("partitionerClass_" + outputIndex, PartitionerClass);

		config.setInteger("partitionerIntParam_" + outputIndex, partitionerParam);

		config.setInteger("numOfOutputs_" + outputIndex,
				componentParallelism.get(downStreamComponentName));

		if (batchSizes.get(upStreamComponentName).get(outputIndex) != null) {
			config.setInteger("batchSize_" + outputIndex, batchSizes.get(upStreamComponentName)
					.get(outputIndex));

		}

	}

	private void putOutputNameToConfig(String upStreamComponentName,
			String downStreamComponentName, int index, Configuration config) {

		String outputName = userDefinedNames.get(downStreamComponentName);
		if (outputName != null) {
			config.setString("outputName_" + (index), outputName);
		}
	}

	public <T extends Tuple> void setOutputSelector(String componentName,
			byte[] serializedOutputSelector) {
		outputSelectors.put(componentName, serializedOutputSelector);

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

	private void buildGraph() {

		for (String componentName : edgeList.keySet()) {
			createVertex(componentName);
		}

		for (String upStreamComponentName : edgeList.keySet()) {
			int i = 0;
			for (String downStreamComponentName : edgeList.get(upStreamComponentName)) {
				connect(upStreamComponentName, downStreamComponentName,
						connectionTypes.get(upStreamComponentName).get(i),
						connectionParams.get(upStreamComponentName).get(i));
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
