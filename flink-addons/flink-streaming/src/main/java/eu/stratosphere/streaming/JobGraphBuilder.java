package eu.stratosphere.streaming;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
 

public class JobGraphBuilder {
	
	private final JobGraph jobGraph;
	private Map<String, JobInputVertex> sourceComponents;
  private Map<String, JobTaskVertex> taskComponents;
  private Map<String, JobOutputVertex> sinkComponents;


	
	public JobGraphBuilder(String jobGraphName) {
   
	  jobGraph = new JobGraph(jobGraphName);
	  sourceComponents = new HashMap<String, JobInputVertex>();
	  taskComponents = new HashMap<String, JobTaskVertex>();
	  sinkComponents = new HashMap<String, JobOutputVertex>();
  }
	
	//TODO: Add source parallelism 
	public void setSource(String sourceName, final Class<? extends AbstractInputTask<?>> sourceClass) {	  
	  
	  final JobInputVertex source = new JobInputVertex(sourceName, jobGraph);
	  source.setInputClass(sourceClass);
	  sourceComponents.put(sourceName, source);
	  
	}
	
	public void setTask(String taskName, final Class<? extends AbstractTask> taskClass, int parallelism) {   
    
    final JobTaskVertex task = new JobTaskVertex(taskName, jobGraph);
    task.setTaskClass(taskClass);
    task.setNumberOfSubtasks(parallelism);
    taskComponents.put(taskName, task);
    
  }
	
public void setSink(String sinkName, final Class<? extends AbstractOutputTask> sinkClass) {    
    
    final JobOutputVertex sink = new JobOutputVertex(sinkName, jobGraph);
    sink.setOutputClass(sinkClass);
    sinkComponents.put(sinkName, sink);
    
  }

public void connectSource(String upStreamComponentName, String downStreamComponentName, ChannelType channelType) {
  
  try {
    JobInputVertex upStreamComponent = sourceComponents.get(upStreamComponentName);
    JobTaskVertex  downStreamComponent = taskComponents.get(downStreamComponentName);
    upStreamComponent.connectTo(downStreamComponent, channelType);
  }
  catch (JobGraphDefinitionException e) {
    e.printStackTrace();
  }
  
}

public void connectTasks(String upStreamComponentName, String downStreamComponentName, ChannelType channelType) {
  
  try {
    JobTaskVertex upStreamComponent = taskComponents.get(upStreamComponentName);
    JobTaskVertex  downStreamComponent = taskComponents.get(downStreamComponentName);
    upStreamComponent.connectTo(downStreamComponent, channelType);
  }
  catch (JobGraphDefinitionException e) {
    e.printStackTrace();
  }
  
}

public void connectSink(String upStreamComponentName, String downStreamComponentName, ChannelType channelType) {
  
  try {
    JobTaskVertex upStreamComponent = taskComponents.get(upStreamComponentName);
    JobOutputVertex downStreamComponent = sinkComponents.get(downStreamComponentName);
    upStreamComponent.connectTo(downStreamComponent, channelType);
  }
  catch (JobGraphDefinitionException e) {
    e.printStackTrace();
  }
  
}


public JobGraph getJobGraph() {
  return jobGraph; 
}


}
