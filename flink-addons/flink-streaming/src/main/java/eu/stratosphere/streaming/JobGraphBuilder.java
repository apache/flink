package eu.stratosphere.streaming;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
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
  private Map<String, AbstractJobVertex> components;

	
	public JobGraphBuilder(String jobGraphName) {
   
	  jobGraph = new JobGraph(jobGraphName);
	  components=new HashMap<String, AbstractJobVertex>();
  }
	
	//TODO: Add source parallelism 
	public void setSource(String sourceName, final Class<? extends AbstractInputTask<?>> sourceClass) {	  
	  
	  final JobInputVertex source = new JobInputVertex(sourceName, jobGraph);
	  source.setInputClass(sourceClass);
	  components.put(sourceName, source);
	  
	}
	
	public void setTask(String taskName, final Class<? extends AbstractTask> taskClass, int parallelism) {   
    
    final JobTaskVertex task = new JobTaskVertex(taskName, jobGraph);
    task.setTaskClass(taskClass);
    task.setNumberOfSubtasks(parallelism);
    components.put(taskName, task);
  }
	
public void setSink(String sinkName, final Class<? extends AbstractOutputTask> sinkClass) {    
    
    final JobOutputVertex sink = new JobOutputVertex(sinkName, jobGraph);
    sink.setOutputClass(sinkClass);
    components.put(sinkName, sink);
  }


public void connect(String upStreamComponentName, String downStreamComponentName, ChannelType channelType) {
  
  AbstractJobVertex upStreamComponent=null;
  AbstractJobVertex downStreamComponent=null;
  
  upStreamComponent = components.get(upStreamComponentName);
  downStreamComponent = components.get(downStreamComponentName);
  
  try {
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
