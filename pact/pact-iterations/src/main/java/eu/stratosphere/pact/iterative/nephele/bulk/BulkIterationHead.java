package eu.stratosphere.pact.iterative.nephele.bulk;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;

public class BulkIterationHead extends IterationHead {

  private VertexPageRank vRank = new VertexPageRank();

  @Override
  public void finish(MutableObjectIterator<Value> iter, OutputCollectorV2 output) throws Exception {
    forwardRecords(iter, output);
  }

  @Override
  public void processInput(MutableObjectIterator<Value> iter, OutputCollectorV2 output) throws Exception {
    forwardRecords(iter, output);
  }

  @Override
  public void processUpdates(MutableObjectIterator<Value> iter, OutputCollectorV2 output) throws Exception {
    forwardRecords(iter, output);
  }

  private final void forwardRecords(MutableObjectIterator<Value> iter, OutputCollectorV2 output) throws Exception {
    while (iter.next(vRank)) {
      output.collect(vRank);
    }
  }
}
