package eu.stratosphere.streaming.test;

import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;

import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.StringValue;

public class WordCount extends TestBase2 {

  @Override
  public JobGraph getJobGraph() {
    JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
    graphBuilder.setSource("WorCountSource", WordCountSource.class);
    graphBuilder.setTask("WorCountTask", WordCountTask.class, 2);
    graphBuilder.setSink("WordCountSink", WordCountSink.class);
    
    graphBuilder.fieldsConnect("WorCountSource", "WorCountTask", 0, StringValue.class);
    graphBuilder.broadcastConnect("WorCountTask", "WordCountSink");

    return graphBuilder.getJobGraph();
  }

}
