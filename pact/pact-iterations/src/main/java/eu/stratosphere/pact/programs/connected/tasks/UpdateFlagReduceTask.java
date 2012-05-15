package eu.stratosphere.pact.programs.connected.tasks;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.SortingReduce;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdateFlag;
import eu.stratosphere.pact.programs.connected.types.PactBoolean;

public class UpdateFlagReduceTask extends SortingReduce {
  ComponentUpdateFlag result = new ComponentUpdateFlag();

  @Override
  public void reduce(Iterator<PactRecord> records, OutputCollectorV2 out)
      throws Exception {
    PactRecord rec = new PactRecord();
    PactLong number = new PactLong();
    PactBoolean updated = new PactBoolean();

    long newCid = Long.MAX_VALUE;
    boolean wasUpdated = false;
    while (records.hasNext()) {
      rec = records.next();
      number = rec.getField(1, number);
      updated = rec.getField(2, updated);

      if (number.getValue() < newCid) {
        newCid = number.getValue();
        wasUpdated = updated.isValue();
      }
      else if (wasUpdated && !updated.isValue() && number.getValue() == newCid) {
        wasUpdated = false;
      }
    }

    result.setCid(newCid);
    result.setVid(rec.getField(0, number).getValue());
    result.setUpdated(wasUpdated);
    out.collect(result);
  }

  @Override
  public PactRecordIterator getInputIterator(IterationIterator iterationIter) {
    return new PactRecordIterator(iterationIter) {
      ComponentUpdateFlag update = new ComponentUpdateFlag();

      PactLong vid = new PactLong();
      PactLong cid = new PactLong();
      PactBoolean updated = new PactBoolean();

      @Override
      public boolean nextPactRecord(MutableObjectIterator<Value> iter,
          PactRecord target) throws IOException {
        boolean success = iter.next(update);

        if (success) {
          vid.setValue(update.getVid());
          cid.setValue(update.getCid());
          updated.setValue(update.isUpdated());
          target.setField(0, vid);
          target.setField(1, cid);
          target.setField(2, updated);
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
