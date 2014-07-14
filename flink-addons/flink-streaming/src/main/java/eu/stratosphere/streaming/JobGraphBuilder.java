package eu.stratosphere.streaming;

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
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.streaming.partitioner.DefaultPartitioner;
import eu.stratosphere.types.Record;

public class JobGraphBuilder {

	private final JobGraph jobGraph;
	private Map<String, AbstractJobVertex> components;
	
	public enum Partitioning {
	   BROADCAST
	}
	
	private Class<? extends ChannelSelector<Record>> getPartitioningClass(Partitioning partitioning) {
	  switch(partitioning) {
	  case BROADCAST:
	    return DefaultPartitioner.class;
	  default:
	    return DefaultPartitioner.class;
	  }
	}
	
	public JobGraphBuilder(String jobGraphName) {

		jobGraph = new JobGraph(jobGraphName);
		components = new HashMap<String, AbstractJobVertex>();
	}

	// TODO: Add source parallelism
	public void setSource(String sourceName,
			final Class<? extends AbstractInputTask<?>> sourceClass) {

		final JobInputVertex source = new JobInputVertex(sourceName, jobGraph);
		source.setInputClass(sourceClass);
		components.put(sourceName, source);

	}
	
	
	
	public void setSource(String sourceName,
      final Class<? extends UserSourceInvokable> InvokableClass, Partitioning partitionType) {

		final JobInputVertex source = new JobInputVertex(sourceName, jobGraph);
		source.setInputClass(StreamSource.class);
		Configuration config = new TaskConfig(source.getConfiguration()).getConfiguration();
		config.setClass("partitioner", getPartitioningClass(partitionType));
		config.setClass("userfunction", InvokableClass);
		components.put(sourceName, source);

	}
	
	public void setTask(String taskName,
	     final Class<? extends UserTaskInvokable> InvokableClass, Partitioning partitionType, int parallelism) {
		
		final JobTaskVertex task = new JobTaskVertex(taskName, jobGraph);
		task.setTaskClass(StreamTask.class);
		task.setNumberOfSubtasks(parallelism);
		Configuration config = new TaskConfig(task.getConfiguration()).getConfiguration();
		config.setClass("partitioner", getPartitioningClass(partitionType));
		config.setClass("userfunction", InvokableClass);
		components.put(taskName, task);
	}

	public void setSink(String sinkName,
			final Class<? extends AbstractOutputTask> sinkClass) {

		final JobOutputVertex sink = new JobOutputVertex(sinkName, jobGraph);
		sink.setOutputClass(sinkClass);
		components.put(sinkName, sink);
	}

	public void connect(String upStreamComponentName,
			String downStreamComponentName, ChannelType channelType) {

		AbstractJobVertex upStreamComponent = null;
		AbstractJobVertex downStreamComponent = null;

		upStreamComponent = components.get(upStreamComponentName);
		downStreamComponent = components.get(downStreamComponentName);

		try {
			upStreamComponent.connectTo(downStreamComponent, channelType);
		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
		}
	}

	public JobGraph getJobGraph() {
		return jobGraph;
	}

}
