package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.Iterator;

@ReduceContract.Combinable
public class DotProductReducer extends ReduceStub {

  private PactRecord accumulator;

  @Override
  public void open(Configuration parameters) throws Exception {
    accumulator = new PactRecord();
  }

  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector) throws Exception {

    PactRecord first = records.next();

    //StringBuilder buffer = new StringBuilder();
    //buffer.append("\t" + first.getField(0, PactLong.class) + " " + first.getField(1, PactDouble.class) + "\n");

    accumulator.setField(0, first.getField(0, PactLong.class));
    double sum = first.getField(1, PactDouble.class).getValue();

    while (records.hasNext()) {
      PactRecord record = records.next();
      sum += record.getField(1, PactDouble.class).getValue();
      //buffer.append("\t" + record.getField(0, PactLong.class) + " " + record.getField(1, PactDouble.class) + "\n");
    }

    accumulator.setField(1, new PactDouble(sum));

    //buffer.append("Reduce: " + accumulator.getField(0, PactLong.class) + " " + sum);
    //System.out.println(buffer);

    collector.collect(accumulator);
  }

  @Override
  public void close() throws Exception {
    accumulator = null;
  }
}
