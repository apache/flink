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
import eu.stratosphere.streaming.partitioner.BroadcastPartitioner;
import eu.stratosphere.streaming.partitioner.FieldsPartitioner;
import eu.stratosphere.streaming.partitioner.GlobalPartitioner;
import eu.stratosphere.streaming.partitioner.ShufflePartitioner;
import eu.stratosphere.types.Key;

public class JobGraphBuilder {

  private final JobGraph jobGraph;
  private Map<String, AbstractJobVertex> components;

  public JobGraphBuilder(String jobGraphName) {

    jobGraph = new JobGraph(jobGraphName);
    components = new HashMap<String, AbstractJobVertex>();
  }

  // TODO: Add source parallelism
  public void setSource(String sourceName,
      final Class<? extends UserSourceInvokable> InvokableClass) {

    final JobInputVertex source = new JobInputVertex(sourceName, jobGraph);
    source.setInputClass(StreamSource.class);
    Configuration config = new TaskConfig(source.getConfiguration())
        .getConfiguration();
    config.setClass("userfunction", InvokableClass);
    components.put(sourceName, source);
  }

  public void setTask(String taskName,
      final Class<? extends UserTaskInvokable> InvokableClass, int parallelism) {

    final JobTaskVertex task = new JobTaskVertex(taskName, jobGraph);
    task.setTaskClass(StreamTask.class);
    task.setNumberOfSubtasks(parallelism);
    Configuration config = new TaskConfig(task.getConfiguration())
        .getConfiguration();
    config.setClass("userfunction", InvokableClass);
    components.put(taskName, task);
  }

  public void setSink(String sinkName,
      final Class<? extends UserSinkInvokable> InvokableClass) {

    final JobOutputVertex sink = new JobOutputVertex(sinkName, jobGraph);
    sink.setOutputClass(StreamSink.class);
    Configuration config = new TaskConfig(sink.getConfiguration())
        .getConfiguration();
    config.setClass("userfunction", InvokableClass);
    components.put(sinkName, sink);
  }

  private void connect(String upStreamComponentName,
      String downStreamComponentName,
      Class<? extends ChannelSelector<StreamRecord>> PartitionerClass,
      ChannelType channelType) {

    AbstractJobVertex upStreamComponent = components.get(upStreamComponentName);
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

    } catch (JobGraphDefinitionException e) {
      e.printStackTrace();
    }
  }

  public void broadcastConnect(String upStreamComponentName,
      String downStreamComponentName) {

    connect(upStreamComponentName, downStreamComponentName,
        BroadcastPartitioner.class, ChannelType.INMEMORY);
  }

  public void fieldsConnect(String upStreamComponentName,
      String downStreamComponentName, int keyPosition,
      Class<? extends Key> keyClass) {

    AbstractJobVertex upStreamComponent = components.get(upStreamComponentName);
    AbstractJobVertex downStreamComponent = components
        .get(downStreamComponentName);

    try {
      upStreamComponent.connectTo(downStreamComponent, ChannelType.INMEMORY);

      Configuration config = new TaskConfig(
          upStreamComponent.getConfiguration()).getConfiguration();

      config.setClass(
          "partitionerClass_"
              + upStreamComponent.getNumberOfForwardConnections(),
          FieldsPartitioner.class);

      config.setClass(
          "partitionerClassParam_"
              + upStreamComponent.getNumberOfForwardConnections(), keyClass);

      config.setInteger(
          "partitionerIntParam_"
              + upStreamComponent.getNumberOfForwardConnections(), keyPosition);

    } catch (JobGraphDefinitionException e) {
      e.printStackTrace();
    }
  }

  public void globalConnect(String upStreamComponentName,
      String downStreamComponentName) {

    connect(upStreamComponentName, downStreamComponentName,
        GlobalPartitioner.class, ChannelType.INMEMORY);
  }

  public void shuffleConnect(String upStreamComponentName,
      String downStreamComponentName) {

    connect(upStreamComponentName, downStreamComponentName,
        ShufflePartitioner.class, ChannelType.INMEMORY);
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
  }

  public JobGraph getJobGraph() {
    setNumberOfJobInputs();
    setNumberOfJobOutputs();
    return jobGraph;
  }

}
