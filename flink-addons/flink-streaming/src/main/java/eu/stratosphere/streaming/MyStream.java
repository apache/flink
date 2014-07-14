package eu.stratosphere.streaming;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.streaming.JobGraphBuilder.Partitioning;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class MyStream extends TestBase2 {
  
public static class MySink extends AbstractOutputTask {
    private RecordReader<Record> input = null;

    @Override
    public void registerInputOutput() {
      this.input = new RecordReader<Record>(this, Record.class);
    }

    @Override
    public void invoke() throws Exception {
      while (input.hasNext()) {
      	StringValue value = new StringValue("");
      	Record record = input.next();
      	record.getFieldInto(0, value);
        System.out.println(value.getValue());
      }
    }
  }
  
  @Override
  public JobGraph getJobGraph() {
    JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
    graphBuilder.setSource("infoSource", TestSourceInvokable.class, Partitioning.BROADCAST);
    graphBuilder.setSource("querySource", QuerySourceInvokable.class, Partitioning.BROADCAST);
    graphBuilder.setTask("cellTask", StreamTask.class, 2);
    graphBuilder.setSink("sink", MySink.class);
    
    graphBuilder.connect("infoSource", "cellTask", ChannelType.INMEMORY);
    graphBuilder.connect("querySource", "cellTask", ChannelType.INMEMORY);
    graphBuilder.connect("cellTask", "sink", ChannelType.INMEMORY);

    return graphBuilder.getJobGraph();
  }

}
