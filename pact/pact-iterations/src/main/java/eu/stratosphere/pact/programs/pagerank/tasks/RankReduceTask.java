package eu.stratosphere.pact.programs.pagerank.tasks;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.SortingReduce;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.iterative.nephele.util.OutputCollectorV2;
import eu.stratosphere.pact.iterative.nephele.util.PactRecordIterator;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;

public class RankReduceTask extends SortingReduce {
  VertexPageRank result = new VertexPageRank();

  @Override
  public void reduce(Iterator<PactRecord> records, OutputCollectorV2 out)
      throws Exception {
    PactRecord rec = new PactRecord();
    PactLong vid = new PactLong();
    PactDouble partial = new PactDouble();

    double contribSum = 0;
    while (records.hasNext()) {
      rec = records.next();
      contribSum += rec.getField(1, partial).getValue();
    }

    double rank = 0.15 / 14052 + 0.85 * contribSum;

    result.setVid(rec.getField(0, vid).getValue());
    result.setRank(rank);
    out.collect(result);
  }

  @Override
  public PactRecordIterator getInputIterator(IterationIterator iterationIter) {
    return new PactRecordIterator(iterationIter) {
      VertexPageRank vRank = new VertexPageRank();

      PactLong vid = new PactLong();
      PactDouble partial = new PactDouble();

      @Override
      public boolean nextPactRecord(MutableObjectIterator<Value> iter,
          PactRecord target) throws IOException {
        boolean success = iter.next(vRank);

        if (success) {
          vid.setValue(vRank.getVid());
          partial.setValue(vRank.getRank());
          target.setField(0, vid);
          target.setField(1, partial);
          return true;
        } else {
          return false;
        }
      }

    };
  }

  @Override
  public int[] getKeyPos() {
    return new int[] {0};
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends Key>[] getKeyClasses() {
    return new Class[] {PactLong.class};
  }

}
