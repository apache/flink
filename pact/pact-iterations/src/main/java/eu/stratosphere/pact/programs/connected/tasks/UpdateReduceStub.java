package eu.stratosphere.pact.programs.connected.tasks;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.programs.connected.types.PactBoolean;

public class UpdateReduceStub extends ReduceStub {

  @Override
  public void reduce(Iterator<PactRecord> records, Collector out)
      throws Exception {
    PactRecord rec = new PactRecord();
    PactLong cid = new PactLong();
    PactBoolean updated = new PactBoolean();

    long newCid = Long.MAX_VALUE;
    boolean wasUpdated = false;
    while (records.hasNext()) {
      rec = records.next();
      cid = rec.getField(1, cid);
      updated = rec.getField(2, updated);

      if (cid.getValue() < newCid) {
        newCid = cid.getValue();
        wasUpdated = updated.isValue();
      } else if (wasUpdated && !updated.isValue() && cid.getValue() == newCid) {
        wasUpdated = false;
      }
    }

    cid.setValue(newCid);
    rec.setField(1, cid);
    updated.setValue(wasUpdated);
    rec.setField(2, updated);
    out.collect(rec);
  }

  @Override
  public void combine(Iterator<PactRecord> records, Collector out)
      throws Exception {
    reduce(records, out);
    return;
  }

}
