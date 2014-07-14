/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.streaming.api.invokable.StreamComponent;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamcomponent.StreamSink;
import eu.stratosphere.streaming.api.streamcomponent.StreamSource;
import eu.stratosphere.streaming.api.streamcomponent.StreamTask;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.partitioner.BroadcastPartitioner;
import eu.stratosphere.streaming.partitioner.FieldsPartitioner;
import eu.stratosphere.streaming.partitioner.GlobalPartitioner;
import eu.stratosphere.streaming.partitioner.ShufflePartitioner;

/**
 * Object for building Stratosphere stream processing job graphs
 */
public class JobGraphBuilder {

	private static final Log log = LogFactory.getLog(JobGraphBuilder.class);
	private final JobGraph jobGraph;
	protected Map<String, AbstractJobVertex> components;
	protected Map<String, Integer> numberOfInstances;
	protected Map<String, List<Integer>> numberOfOutputChannels;
	protected String maxParallelismVertexName;
	protected int maxParallelism;
	protected FaultToleranceType faultToleranceType;
	private int batchSize;
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
		numberOfInstances = new HashMap<String, Integer>();
		numberOfOutputChannels = new HashMap<String, List<Integer>>();
		maxParallelismVertexName = "";
		maxParallelism = 0;
		if (log.isDebugEnabled()) {
			log.debug("JobGraph created");
		}
		this.faultToleranceType = faultToleranceType;
		batchSize = 1;
	}

	/**
	 * Creates a new JobGraph with the given name with fault tolerance turned
	 * off
	 * 
	 * @param jobGraphName
	 *            Name of the JobGraph
	 */
	public JobGraphBuilder(String jobGraphName) {
		this(jobGraphName, FaultToleranceType.NONE);
	}

	public JobGraphBuilder(String jobGraphName, FaultToleranceType faultToleranceType, int batchSize) {
		this(jobGraphName,faultToleranceType);
		this.batchSize = batchSize;
	}

	/**
	 * Adds source to the JobGraph by user defined object with no parallelism
	 * 
	 * @param sourceName
	 *            Name of the source component
	 * @param InvokableObject
	 *            User defined UserSourceInvokable object or other predefined
	 *            source object
	 */
	public void setSource(String sourceName, UserSourceInvokable<? extends Tuple> InvokableObject,
			String operatorName, byte[] serializedFunction) {
		Configuration config = setSource(sourceName, InvokableObject, 1, 1);
		config.setBytes("operator", serializedFunction);
		config.setString("operatorName", operatorName);
	}

	/**
	 * Adds source to the JobGraph by user defined object with the set
	 * parallelism
	 * 
	 * @param sourceName
	 *            Name of the source component
	 * @param InvokableObject
	 *            User defined UserSourceInvokable object or other predefined
	 *            source object
	 * @param parallelism
	 *            Number of task instances of this type to run in parallel
	 * @param subtasksPerInstance
	 *            Number of subtasks allocated to a machine
	 */
	public Configuration setSource(String sourceName,
			UserSourceInvokable<? extends Tuple> InvokableObject, int parallelism,
			int subtasksPerInstance) {
		final JobInputVertex source = new JobInputVertex(sourceName, jobGraph);
		source.setInputClass(StreamSource.class);
		Configuration config = setComponent(sourceName, InvokableObject, parallelism,
				subtasksPerInstance, source);
		if (log.isDebugEnabled()) {
			log.debug("SOURCE: " + sourceName);
		}
		return config;
	}

	public void setTask(String taskName,
			UserTaskInvokable<? extends Tuple, ? extends Tuple> TaskInvokableObject,
			String operatorName, byte[] serializedFunction) {
		Configuration config = setTask(taskName, TaskInvokableObject, 1, 1);
		config.setBytes("operator", serializedFunction);
		config.setString("operatorName", operatorName);
	}

	/**
	 * Adds a task component to the JobGraph with no parallelism
	 * 
	 * @param taskName
	 *            Name of the task component
	 * @param TaskInvokableObject
	 *            User defined UserTaskInvokable object
	 */
	public void setTask(String taskName,
			UserTaskInvokable<? extends Tuple, ? extends Tuple> TaskInvokableObject) {
		setTask(taskName, TaskInvokableObject, 1, 1);
	}

	/**
	 * Adds a task component to the JobGraph
	 * 
	 * @param taskName
	 *            Name of the task component
	 * @param TaskInvokableObject
	 *            User defined UserTaskInvokable object
	 * @param parallelism
	 *            Number of task instances of this type to run in parallel
	 * @param subtasksPerInstance
	 *            Number of subtasks allocated to a machine
	 * @return
	 */
	public Configuration setTask(String taskName,
			UserTaskInvokable<? extends Tuple, ? extends Tuple> TaskInvokableObject,
			int parallelism, int subtasksPerInstance) {
		final JobTaskVertex task = new JobTaskVertex(taskName, jobGraph);
		task.setTaskClass(StreamTask.class);
		Configuration config = setComponent(taskName, TaskInvokableObject, parallelism,
				subtasksPerInstance, task);
		if (log.isDebugEnabled()) {
			log.debug("TASK: " + taskName);
		}
		return config;
	}

	public void setSink(String sinkName, UserSinkInvokable<? extends Tuple> InvokableObject,
			String operatorName, byte[] serializedFunction) {
		Configuration config = setSink(sinkName, InvokableObject, 1, 1);
		config.setBytes("operator", serializedFunction);
		config.setString("operatorName", operatorName);

	}

	/**
	 * Adds a sink component to the JobGraph with no parallelism
	 * 
	 * @param sinkName
	 *            Name of the sink component
	 * @param InvokableObject
	 *            User defined UserSinkInvokable object
	 * @param parallelism
	 *            Number of task instances of this type to run in parallel
	 * @param subtasksPerInstance
	 *            Number of subtasks allocated to a machine
	 */
	public Configuration setSink(String sinkName,
			UserSinkInvokable<? extends Tuple> InvokableObject, int parallelism,
			int subtasksPerInstance) {
		final JobOutputVertex sink = new JobOutputVertex(sinkName, jobGraph);
		sink.setOutputClass(StreamSink.class);
		Configuration config = setComponent(sinkName, InvokableObject, parallelism,
				subtasksPerInstance, sink);
		if (log.isDebugEnabled()) {
			log.debug("SINK: " + sinkName);
		}
		return config;
	}

	/**
	 * Sets JobVertex configuration based on the given parameters
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param InvokableClass
	 *            Class of the user defined Invokable
	 * @param parallelism
	 *            Number of subtasks
	 * @param subtasksPerInstance
	 *            Number of subtasks per instance
	 * @param component
	 *            AbstractJobVertex associated with the component
	 */

	private Configuration setComponent(String componentName,
			final Class<? extends StreamComponent> InvokableClass, int parallelism,
			int subtasksPerInstance, AbstractJobVertex component) {
		component.setNumberOfSubtasks(parallelism);
		component.setNumberOfSubtasksPerInstance(subtasksPerInstance);

		if (parallelism > maxParallelism) {
			maxParallelism = parallelism;
			maxParallelismVertexName = componentName;
		}

		Configuration config = new TaskConfig(component.getConfiguration()).getConfiguration();
		config.setClass("userfunction", InvokableClass);
		config.setString("componentName", componentName);
		config.setInteger("batchSize", batchSize);
		// config.setBytes("operator", getSerializedFunction());

		config.setInteger("faultToleranceType", faultToleranceType.id);

		components.put(componentName, component);
		numberOfInstances.put(componentName, parallelism);
		return config;
	}

	private Configuration setComponent(String componentName,
			UserSourceInvokable<? extends Tuple> InvokableObject, int parallelism,
			int subtasksPerInstance, AbstractJobVertex component) {
		Configuration config = setComponent(componentName, InvokableObject.getClass(), parallelism,
				subtasksPerInstance, component);

		addSerializedObject(InvokableObject, component);
		return config;
	}

	private Configuration setComponent(String componentName,
			UserTaskInvokable<? extends Tuple, ? extends Tuple> InvokableObject, int parallelism,
			int subtasksPerInstance, AbstractJobVertex component) {
		Configuration config = setComponent(componentName, InvokableObject.getClass(), parallelism,
				subtasksPerInstance, component);

		addSerializedObject(InvokableObject, component);
		return config;
	}

	private Configuration setComponent(String componentName,
			UserSinkInvokable<? extends Tuple> InvokableObject, int parallelism,
			int subtasksPerInstance, AbstractJobVertex component) {
		Configuration config = setComponent(componentName, InvokableObject.getClass(), parallelism,
				subtasksPerInstance, component);

		addSerializedObject(InvokableObject, component);
		return config;
	}

	/**
	 * Adds serialized invokable object to the JobVertex configuration
	 * 
	 * @param InvokableObject
	 *            Invokable object to serialize
	 * @param component
	 *            JobVertex to which the serialized invokable will be added
	 */
	private void addSerializedObject(Serializable InvokableObject, AbstractJobVertex component) {

		Configuration config = component.getConfiguration();

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
	 * Connects to JobGraph components with the given names, partitioning and
	 * channel type
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the records
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the
	 *            records
	 * @param PartitionerClass
	 *            Class of the partitioner
	 * @param channelType
	 *            Channel Type
	 */
	private void connect(String upStreamComponentName, String downStreamComponentName,
			Class<? extends ChannelSelector<StreamRecord>> PartitionerClass) {

		AbstractJobVertex upStreamComponent = components.get(upStreamComponentName);
		AbstractJobVertex downStreamComponent = components.get(downStreamComponentName);

		try {
			upStreamComponent.connectTo(downStreamComponent, ChannelType.NETWORK);
			Configuration config = new TaskConfig(upStreamComponent.getConfiguration())
					.getConfiguration();
			config.setClass(
					"partitionerClass_" + (upStreamComponent.getNumberOfForwardConnections() - 1),
					PartitionerClass);
			if (log.isDebugEnabled()) {
				log.debug("CONNECTED: " + PartitionerClass.getSimpleName() + " - "
						+ upStreamComponentName + " -> " + downStreamComponentName);
			}
		} catch (JobGraphDefinitionException e) {
			if (log.isErrorEnabled()) {
				log.error("Cannot connect components with " + PartitionerClass.getSimpleName()
						+ " : " + upStreamComponentName + " -> " + downStreamComponentName, e);
			}
		}
	}

	/**
	 * Sets instance sharing between the given components
	 * 
	 * @param component1
	 *            Share will be called on this component
	 * @param component2
	 */
	public void setInstanceSharing(String component1, String component2) {
		AbstractJobVertex c1 = components.get(component1);
		AbstractJobVertex c2 = components.get(component2);

		c1.setVertexToShareInstancesWith(c2);
	}

	/**
	 * Sets all components to share with the one with highest parallelism
	 */
	public void setAutomaticInstanceSharing() {

		AbstractJobVertex maxParallelismVertex = components.get(maxParallelismVertexName);

		for (String componentName : components.keySet()) {
			if (componentName != maxParallelismVertexName) {
				components.get(componentName).setVertexToShareInstancesWith(maxParallelismVertex);
			}
		}

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
		connect(upStreamComponentName, downStreamComponentName, BroadcastPartitioner.class);
		addOutputChannels(upStreamComponentName, numberOfInstances.get(downStreamComponentName));
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

		AbstractJobVertex upStreamComponent = components.get(upStreamComponentName);
		AbstractJobVertex downStreamComponent = components.get(downStreamComponentName);

		try {
			upStreamComponent.connectTo(downStreamComponent, ChannelType.NETWORK);

			Configuration config = new TaskConfig(upStreamComponent.getConfiguration())
					.getConfiguration();

			config.setClass(
					"partitionerClass_" + (upStreamComponent.getNumberOfForwardConnections() - 1),
					FieldsPartitioner.class);

			config.setInteger(
					"partitionerIntParam_"
							+ (upStreamComponent.getNumberOfForwardConnections() - 1), keyPosition);

			addOutputChannels(upStreamComponentName, 1);
			if (log.isDebugEnabled()) {
				log.debug("CONNECTED: FIELD PARTITIONING - " + upStreamComponentName + " -> "
						+ downStreamComponentName + ", KEY: " + keyPosition);
			}
		} catch (JobGraphDefinitionException e) {
			if (log.isErrorEnabled()) {
				log.error("Cannot connect components by field: " + upStreamComponentName + " to "
						+ downStreamComponentName, e);
			}
		}
		log.info("Fieldsconnected " + upStreamComponentName + " to " + downStreamComponentName
				+ " on " + keyPosition);

	}

	/**
	 * Connects two components with the given names by global partitioning.
	 * <p>
	 * Global partitioning: sends all emitted records to one output instance
	 * (i.e. the first one)
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the records
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the
	 *            records
	 */
	public void globalConnect(String upStreamComponentName, String downStreamComponentName) {
		connect(upStreamComponentName, downStreamComponentName, GlobalPartitioner.class);
		addOutputChannels(upStreamComponentName, 1);
		log.info("Globalconnected: " + upStreamComponentName + " to " + downStreamComponentName);

	}

	/**
	 * Connects two components with the given names by shuffle partitioning.
	 * <p>
	 * Shuffle partitioning: sends the output records to a randomly selected
	 * channel
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the records
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the
	 *            records
	 */
	public void shuffleConnect(String upStreamComponentName, String downStreamComponentName) {
		connect(upStreamComponentName, downStreamComponentName, ShufflePartitioner.class);
		addOutputChannels(upStreamComponentName, 1);
		log.info("Shuffleconnected: " + upStreamComponentName + " to " + downStreamComponentName);
	}

	private void addOutputChannels(String upStreamComponentName, int numOfInstances) {
		if (numberOfOutputChannels.containsKey(upStreamComponentName)) {
			numberOfOutputChannels.get(upStreamComponentName).add(numOfInstances);
		} else {
			numberOfOutputChannels.put(upStreamComponentName, new ArrayList<Integer>());
			numberOfOutputChannels.get(upStreamComponentName).add(numOfInstances);
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

		for (String component : numberOfOutputChannels.keySet()) {
			Configuration config = components.get(component).getConfiguration();
			List<Integer> channelNumList = numberOfOutputChannels.get(component);
			for (int i = 0; i < channelNumList.size(); i++) {
				config.setInteger("channels_" + i, channelNumList.get(i));
			}
		}
	}

	/**
	 * Returns the JobGraph
	 * 
	 * @return JobGraph object
	 */
	public JobGraph getJobGraph() {
		setAutomaticInstanceSharing();
		setNumberOfJobInputs();
		setNumberOfJobOutputs();
		return jobGraph;
	}
}
