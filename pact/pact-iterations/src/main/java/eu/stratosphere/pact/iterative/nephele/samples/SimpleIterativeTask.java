package eu.stratosphere.pact.iterative.nephele.samples;


import java.io.IOException;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class SimpleIterativeTask {

  public static void main(String[] args) throws Exception {
/*    if (args.length != 3) {
      System.exit(-1);
    }

    final int dop = Integer.parseInt(args[0]);
    final String input = args[1];
    final String output = args[2];*/

    final int dop = 1;
    final String input = "file:///home/ssc/Desktop/strato/data/edges.txt";
    final String output = "file:///home/ssc/Desktop/strato/data/out/";

    JobGraph graph = new JobGraph("Iterative Test");

    //Create tasks
    JobInputVertex sourceVertex = NepheleUtil.createInput(EdgeInput.class, input, graph, dop);

    JobTaskVertex iterationStart = NepheleUtil.createTask(DummyIterationHead.class, graph, dop);

    JobTaskVertex forward = NepheleUtil.createTask(DummyIterativeForward.class, graph, dop);
    forward.setVertexToShareInstancesWith(sourceVertex);

    JobOutputVertex sinkVertex = NepheleUtil.createOutput(EdgeOutput.class, output, graph, dop);
    sinkVertex.setVertexToShareInstancesWith(sourceVertex);

    NepheleUtil.connectBoundedRoundsIterationLoop(sourceVertex, sinkVertex, new JobTaskVertex[] { forward }, forward,
        iterationStart, ShipStrategy.FORWARD, 100, graph);

    graph.addJar(new Path("/home/ssc/Entwicklung/projects/stratosphere-iterations/pact/pact-iterations/target/pact-iterations-0.2.jar"));
    graph.addJar(new Path("/home/ssc/.m2/repository/com/google/guava/guava/r09/guava-r09.jar"));
    NepheleUtil.submit(graph, NepheleUtil.getConfiguration());
  }

  public static class EdgeInput extends DelimitedInputFormat {

    private static final Pattern SEPARATOR = Pattern.compile(",");

    @Override
    public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
      String[] ids = SEPARATOR.split(new String(bytes, Charsets.UTF_8));
      System.out.println(">>" + new String(bytes, Charsets.UTF_8) +"<< --> " + ids[0] + " " + ids[1]);
      try {
        target.setField(0, new PactInteger(Integer.parseInt(ids[0])));
        target.setField(1, new PactInteger(Integer.parseInt(ids[1])));
      } catch (Exception e) {
        target.setField(0, new PactInteger(1));
        target.setField(1, new PactInteger(1));
      }
      return true;
    }
  }

  public static class EdgeOutput extends FileOutputFormat {

    public EdgeOutput() {}

    @Override
    public void writeRecord(PactRecord record) throws IOException {
      PactInteger a = record.getField(0, PactInteger.class);
      PactInteger b = record.getField(1, PactInteger.class);
      stream.write((a.getValue() + "," + b.getValue() + "\n").getBytes(Charsets.UTF_8));
    }
  }


  public static class DummyIterationHead extends IterationHead {

    private PactRecord record = new PactRecord();

    @Override
    public void finish(MutableObjectIterator<Value> iter, OutputCollector output) throws Exception {
      while (iter.next(record)) {
        output.collect(record);
      }
    }

    @Override
    public void processInput(MutableObjectIterator<Value> iter, Collector output) throws Exception {
      while (iter.next(record)) {}

      //Inject two dummy records in the iteration process
      for (int i = 0; i < 100; i++) {
        record.setField(0, new PactInteger(i));
        output.collect(record);
      }
    }

    @Override
    public void processUpdates(MutableObjectIterator<Value> iter, Collector output) throws Exception {
      PactRecord record = new PactRecord();
      while (iter.next(record)) {
        output.collect(record);
      }
    }

    @Override
    public boolean requiresComparatorOnInput() {
      return false;
    }
  }

  public static class DummyIterativeForward extends AbstractIterativeTask {

    private PactRecord record = new PactRecord();

    @Override
    public void cleanup() throws Exception {}

    @Override
    public void runIteration(IterationIterator iterationIter)
        throws Exception {
      while (iterationIter.next(record)) {
        output.collect(record);
      }
    }

    @Override
    public int getNumberOfInputs() {
      return 1;
    }

    @Override
    public boolean requiresComparatorOnInput() {
      return false;
    }

  }
}
