package eu.stratosphere.pact.programs.preparation.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;

public class UndirectedAdjList extends AbstractMinimalTask {
  PactRecord record = new PactRecord();
  PactRecord result = new PactRecord();

  PactLong nid = new PactLong();
  LongList adjList = new LongList();
  PactLong vid = new PactLong();


  @Override
  protected void initTask() {
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

  @Override
  public void run() throws Exception {
    MutableObjectIterator<Value> input = inputs[0];
    int numNeighbours = 0;
    long[] neighbours = null;
    while (input.next(record)) {
      vid = record.getField(0, vid);
      adjList = record.getField(1, adjList);

      numNeighbours = adjList.getLength();
      neighbours = adjList.getList();

      for (int i = 0; i < numNeighbours; i++) {
        nid.setValue(neighbours[i]);

        result.setField(0, vid);
        result.setField(1, nid);
        output.collect(result);

        result.setField(0, nid);
        result.setField(1, vid);
        output.collect(result);
      }
    }
  }

}
