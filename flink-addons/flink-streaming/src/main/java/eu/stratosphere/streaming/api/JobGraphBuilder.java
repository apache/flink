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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamcomponent.StreamSink;
import eu.stratosphere.streaming.api.streamcomponent.StreamSource;
import eu.stratosphere.streaming.api.streamcomponent.StreamTask;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.partitioner.BroadcastPartitioner;
import eu.stratosphere.streaming.partitioner.FieldsPartitioner;
import eu.stratosphere.streaming.partitioner.GlobalPartitioner;
import eu.stratosphere.streaming.partitioner.ShufflePartitioner;
import eu.stratosphere.types.Key;

/**
 * Object for building Stratosphere stream processing job graphs
 * 
 */
public class JobGraphBuilder {

	private static final Log log = LogFactory.getLog(JobGraphBuilder.class);
	private final JobGraph jobGraph;
	private Map<String, AbstractJobVertex> components;
	private Map<String, Integer> numberOfInstances;
	private Map<String, Integer> numberOfOutputChannels;

	/**
	 * Creates a new JobGraph with the given name
	 * 
	 * @param jobGraphName
	 *            Name of the JobGraph
	 */
	public JobGraphBuilder(String jobGraphName) {
		jobGraph = new JobGraph(jobGraphName);
		components = new HashMap<String, AbstractJobVertex>();
		numberOfInstances = new HashMap<String, Integer>();
		numberOfOutputChannels = new HashMap<String, Integer>();
		log.debug("JobGraph created");

	}

	/**
	 * Adds a source component to the JobGraph
	 * 
	 * @param sourceName
	 *            Name of the source component
	 * @param InvokableClass
	 *            User defined class describing the source
	 */
	public void setSource(String sourceName,
			final Class<? extends UserSourceInvokable> InvokableClass) {

		final JobInputVertex source = new JobInputVertex(sourceName, jobGraph);
		source.setInputClass(StreamSource.class);
		Configuration config = new TaskConfig(source.getConfiguration())
				.getConfiguration();
		config.setClass("userfunction", InvokableClass);
		config.setString("componentName", sourceName);
		components.put(sourceName, source);
		numberOfInstances.put(sourceName, 1);
		log.debug("Source set: " + sourceName);
	}

	/**
	 * Adds a source component to the JobGraph
	 * 
	 * @param sourceName
	 *            Name of the source component
	 * @param InvokableClass
	 *            User defined class describing the source
	 * @param parallelism
	 *            Number of task instances of this type to run in parallel
	 */
	public void setSource(String sourceName,
			final Class<? extends UserSourceInvokable> InvokableClass,
			int parallelism) {

		final JobInputVertex source = new JobInputVertex(sourceName, jobGraph);
		source.setInputClass(StreamSource.class);
		source.setNumberOfSubtasks(parallelism);
		Configuration config = new TaskConfig(source.getConfiguration())
				.getConfiguration();
		config.setClass("userfunction", InvokableClass);
		config.setString("componentName", sourceName);
		components.put(sourceName, source);
		numberOfInstances.put(sourceName, 1);
		log.debug("Source set: " + sourceName);
	}

	/**
	 * Adds a task component to the JobGraph
	 * 
	 * @param taskName
	 *            Name of the task component
	 * @param InvokableClass
	 *            User defined class describing the task
	 */
	public void setTask(String taskName,
			final Class<? extends UserTaskInvokable> InvokableClass) {

		final JobTaskVertex task = new JobTaskVertex(taskName, jobGraph);
		task.setTaskClass(StreamTask.class);
		Configuration config = new TaskConfig(task.getConfiguration())
				.getConfiguration();
		config.setClass("userfunction", InvokableClass);
		config.setString("componentName", taskName);
		components.put(taskName, task);
		numberOfInstances.put(taskName, 1);
		log.debug("Task set: " + taskName);
	}

	/**
	 * Adds a task component to the JobGraph
	 * 
	 * @param taskName
	 *            Name of the task component
	 * @param InvokableClass
	 *            User defined class describing the task
	 * @param parallelism
	 *            Number of task instances of this type to run in parallel
	 */
	public void setTask(String taskName,
			final Class<? extends UserTaskInvokable> InvokableClass,
			int parallelism) {

		final JobTaskVertex task = new JobTaskVertex(taskName, jobGraph);
		task.setTaskClass(StreamTask.class);
		task.setNumberOfSubtasks(parallelism);
		Configuration config = new TaskConfig(task.getConfiguration())
				.getConfiguration();
		config.setClass("userfunction", InvokableClass);
		config.setString("componentName", taskName);
		components.put(taskName, task);
		numberOfInstances.put(taskName, parallelism);
		log.debug("Task set: " + taskName);
	}

	/**
	 * Adds a sink component to the JobGraph
	 * 
	 * @param sinkName
	 *            Name of the sink component
	 * @param InvokableClass
	 *            User defined class describing the sink
	 */
	public void setSink(String sinkName,
			final Class<? extends UserSinkInvokable> InvokableClass) {

		final JobOutputVertex sink = new JobOutputVertex(sinkName, jobGraph);
		sink.setOutputClass(StreamSink.class);
		Configuration config = new TaskConfig(sink.getConfiguration())
				.getConfiguration();
		config.setClass("userfunction", InvokableClass);
		config.setString("componentName", sinkName);
		components.put(sinkName, sink);
		numberOfInstances.put(sinkName, 1);
		log.debug("Sink set: " + sinkName);
	}

	/**
	 * Adds a sink component to the JobGraph
	 * 
	 * @param sinkName
	 *            Name of the sink component
	 * @param InvokableClass
	 *            User defined class describing the sink
	 * @param parallelism
	 *            Number of task instances of this type to run in parallel
	 */
	public void setSink(String sinkName,
			final Class<? extends UserSinkInvokable> InvokableClass,
			int parallelism) {

		final JobOutputVertex sink = new JobOutputVertex(sinkName, jobGraph);
		sink.setOutputClass(StreamSink.class);
		sink.setNumberOfSubtasks(parallelism);
		Configuration config = new TaskConfig(sink.getConfiguration())
				.getConfiguration();
		config.setClass("userfunction", InvokableClass);
		config.setString("componentName", sinkName);
		components.put(sinkName, sink);
		numberOfInstances.put(sinkName, 1);
		log.debug("Sink set: " + sinkName);
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
	private void connect(String upStreamComponentName,
			String downStreamComponentName,
			Class<? extends ChannelSelector<StreamRecord>> PartitionerClass,
			ChannelType channelType) {

		AbstractJobVertex upStreamComponent = components
				.get(upStreamComponentName);
		AbstractJobVertex downStreamComponent = components
				.get(downStreamComponentName);

		try {
			upStreamComponent.connectTo(downStreamComponent, channelType);
			Configuration config = new TaskConfig(
					upStreamComponent.getConfiguration()).getConfiguration();
			config.setClass(
					"partitionerClass_"
							+ upStreamComponent.getNumberOfForwardConnections(),
					PartitionerClass);
			log.debug("Components connected with "
					+ PartitionerClass.getSimpleName() + ": "
					+ upStreamComponentName + " to " + downStreamComponentName);
		} catch (JobGraphDefinitionException e) {
			log.error(
					"Cannot connect components with "
							+ PartitionerClass.getSimpleName() + " : "
							+ upStreamComponentName + " to "
							+ downStreamComponentName, e);
		}
	}

	/**
	 * Connects two components with the given names by broadcast partitioning.
	 * <p>
	 * Broadcast partitioning: All the emmitted tuples are replicated to all of
	 * the output instances
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the records
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the
	 *            records
	 */
	public void broadcastConnect(String upStreamComponentName,
			String downStreamComponentName) {

		connect(upStreamComponentName, downStreamComponentName,
				BroadcastPartitioner.class, ChannelType.INMEMORY);

		if (numberOfOutputChannels.containsKey(upStreamComponentName)) {
			numberOfOutputChannels.put(upStreamComponentName,
					numberOfOutputChannels.get(upStreamComponentName)
							+ numberOfInstances.get(downStreamComponentName));
		} else {
			numberOfOutputChannels.put(upStreamComponentName,
					numberOfInstances.get(downStreamComponentName));
		}
		// log.debug("Components connected with broadcast: " +
		// upStreamComponentName + " to " + downStreamComponentName);
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
	 *            Position of key in the record
	 * @param keyClass
	 *            Class of the key Value stored in the record
	 */
	public void fieldsConnect(String upStreamComponentName,
			String downStreamComponentName, int keyPosition,
			Class<? extends Key> keyClass) {

		AbstractJobVertex upStreamComponent = components
				.get(upStreamComponentName);
		AbstractJobVertex downStreamComponent = components
				.get(downStreamComponentName);

		try {
			upStreamComponent.connectTo(downStreamComponent,
					ChannelType.INMEMORY);

			Configuration config = new TaskConfig(
					upStreamComponent.getConfiguration()).getConfiguration();

			config.setClass(
					"partitionerClass_"
							+ upStreamComponent.getNumberOfForwardConnections(),
					FieldsPartitioner.class);

			config.setClass(
					"partitionerClassParam_"
							+ upStreamComponent.getNumberOfForwardConnections(),
					keyClass);

			config.setInteger(
					"partitionerIntParam_"
							+ upStreamComponent.getNumberOfForwardConnections(),
					keyPosition);

			addOutputChannels(upStreamComponentName);
			log.debug("Components connected by field: " + upStreamComponentName
					+ " to " + downStreamComponentName + " by key position "
					+ keyPosition);
		} catch (JobGraphDefinitionException e) {
			log.error("Cannot connect components by field: "
					+ upStreamComponentName + " to " + downStreamComponentName,
					e);
		}
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
	public void globalConnect(String upStreamComponentName,
			String downStreamComponentName) {

		connect(upStreamComponentName, downStreamComponentName,
				GlobalPartitioner.class, ChannelType.INMEMORY);

		addOutputChannels(upStreamComponentName);

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
	public void shuffleConnect(String upStreamComponentName,
			String downStreamComponentName) {

		connect(upStreamComponentName, downStreamComponentName,
				ShufflePartitioner.class, ChannelType.INMEMORY);

		addOutputChannels(upStreamComponentName);
	}

	private void addOutputChannels(String upStreamComponentName) {
		if (numberOfOutputChannels.containsKey(upStreamComponentName)) {
			numberOfOutputChannels.put(upStreamComponentName,
					numberOfOutputChannels.get(upStreamComponentName) + 1);
		} else {
			numberOfOutputChannels.put(upStreamComponentName, 1);
		}
	}

	private void setNumberOfJobInputs() {
		for (AbstractJobVertex component : components.values()) {
			component.getConfiguration().setInteger("numberOfInputs",
					component.getNumberOfBackwardConnections());
		}
	}

	private void setNumberOfJobOutputs() {
		for (AbstractJobVertex component : components.values()) {
			component.getConfiguration().setInteger("numberOfOutputs",
					component.getNumberOfForwardConnections());
		}
		for (String component : numberOfOutputChannels.keySet()) {
			components
					.get(component)
					.getConfiguration()
					.setInteger("numberOfOutputChannels",
							numberOfOutputChannels.get(component));
		}
	}

	/**
	 * 
	 * @return The JobGraph object
	 */
	public JobGraph getJobGraph() {
		setNumberOfJobInputs();
		setNumberOfJobOutputs();
		return jobGraph;
	}

}
