package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

import java.util.Iterator;

@ReduceContract.Combinable
public class DotProductReducer extends ReduceStub {

  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector) throws Exception {
    PactRecord accumulator = records.next();
    PactDouble rank = accumulator.getField(1, PactDouble.class);
    double sum = rank.getValue();
    while (records.hasNext()) {
      sum += records.next().getField(1, PactDouble.class).getValue();
    }
    rank.setValue(sum);
    accumulator.setField(1, rank);
  }
}
