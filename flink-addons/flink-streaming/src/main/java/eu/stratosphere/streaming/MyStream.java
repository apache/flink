package eu.stratosphere.streaming;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.JobGraphBuilder.Partitioning;
import eu.stratosphere.test.util.TestBase2;

public class MyStream extends TestBase2 {
  
  @Override
  public JobGraph getJobGraph() {
    JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
    graphBuilder.setSource("infoSource", TestSourceInvokable.class, Partitioning.BROADCAST);
    graphBuilder.setSource("querySource", QuerySourceInvokable.class, Partitioning.BROADCAST);
    graphBuilder.setTask("cellTask", TestTaskInvokable.class, Partitioning.BROADCAST, 2);
    graphBuilder.setSink("sink", TestSinkInvokable.class);
    
    graphBuilder.connect("infoSource", "cellTask", ChannelType.INMEMORY);
    graphBuilder.connect("querySource", "cellTask", ChannelType.INMEMORY);
    graphBuilder.connect("cellTask", "sink", ChannelType.INMEMORY);

    return graphBuilder.getJobGraph();
  }

}
