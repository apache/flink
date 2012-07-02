package eu.stratosphere.pact.runtime.iterative.task;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;

public class EmptyMapStub extends MapStub {
  @Override
  public void map(PactRecord record, Collector<PactRecord> out) throws Exception {}
}
