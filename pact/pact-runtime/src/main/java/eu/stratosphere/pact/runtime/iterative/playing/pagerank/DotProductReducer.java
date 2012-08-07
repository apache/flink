package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.Iterator;

@ReduceContract.Combinable
public class DotProductReducer extends ReduceStub {

  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector) throws Exception {

    PactRecord accumulator = new PactRecord();
    double sum = 0;
    while (records.hasNext()) {
      PactRecord record = records.next();
      accumulator.setField(0, record.getField(0, PactLong.class));
      sum += record.getField(1, PactDouble.class).getValue();
      //System.out.println("\t" + record.getField(0, PactLong.class) + " " + record.getField(1, PactDouble.class));
    }

    accumulator.setField(1, new PactDouble(sum));

    //System.out.println("Reduce: " + accumulator.getField(0, PactLong.class) + " " + sum);

    collector.collect(accumulator);
  }
}
