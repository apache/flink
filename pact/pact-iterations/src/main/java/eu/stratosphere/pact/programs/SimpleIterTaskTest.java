package eu.stratosphere.pact.programs;

import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectBoundedRoundsIterationLoop;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createInput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createOutput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createTask;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.getConfiguration;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.submit;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.io.EdgeInput;
import eu.stratosphere.pact.iterative.nephele.io.EdgeOutput;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.iterative.nephele.util.OutputCollectorV2;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class SimpleIterTaskTest {
  public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
  {
    if (args.length != 3) {
      System.exit(-1);
    }

    final int dop = Integer.parseInt(args[0]);
    final String input = args[1];
    final String output = args[2];

    JobGraph graph = new JobGraph("Iterative Test");

    //Create tasks
    JobInputVertex sourceVertex = createInput(EdgeInput.class, input, graph, dop);

    JobTaskVertex iterationStart = createTask(DummyIterationHead.class, graph, dop);
    iterationStart.setVertexToShareInstancesWith(sourceVertex);

    JobTaskVertex forward = createTask(DummyIterativeForward.class, graph, dop);
    forward.setVertexToShareInstancesWith(sourceVertex);

    JobOutputVertex sinkVertex = createOutput(EdgeOutput.class, output, graph, dop);
    sinkVertex.setVertexToShareInstancesWith(sourceVertex);

    connectBoundedRoundsIterationLoop(sourceVertex, sinkVertex, new JobTaskVertex[] {forward}, forward,
        iterationStart, ShipStrategy.FORWARD, 100, graph);

    //Submit job
    submit(graph, getConfiguration());
  }

  public static class DummyIterationHead extends IterationHead {
    private PactRecord rec = new PactRecord();

    @Override
    public void finish(MutableObjectIterator<Value> iter,
        OutputCollectorV2 output) throws Exception {
      while (iter.next(rec)) {
        output.collect(rec);
      }
    }

    @Override
    public void processInput(MutableObjectIterator<Value> iter,
        OutputCollectorV2 output) throws Exception {

      while (iter.next(rec)) {
      }

      //Inject two dummy records in the iteration process
      for (int i = 0; i < 100; i++) {
        rec.setField(0, new PactInteger(i));
        output.collect(rec);
      }
    }

    @Override
    public void processUpdates(MutableObjectIterator<Value> iter,
        OutputCollectorV2 output) throws Exception {
      PactRecord rec = new PactRecord();
      while (iter.next(rec)) {
        output.collect(rec);
      }
    }

  }

  public static class DummyIterativeForward extends AbstractIterativeTask {
    private PactRecord rec = new PactRecord();

    @Override
    public void cleanup() throws Exception {
    }

    @Override
    public void runIteration(IterationIterator iterationIter)
        throws Exception {
      while (iterationIter.next(rec)) {
        output.collect(rec);
      }
    }

    @Override
    protected void initTask() {
    }

    @Override
    public int getNumberOfInputs() {
      return 1;
    }

  }
}
