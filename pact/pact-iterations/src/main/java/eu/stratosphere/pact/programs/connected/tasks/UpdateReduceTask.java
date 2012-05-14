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
import eu.stratosphere.pact.iterative.nephele.util.OutputCollectorV2;
import eu.stratosphere.pact.iterative.nephele.util.PactRecordIterator;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdate;

public class UpdateReduceTask extends SortingReduce {
  ComponentUpdate result = new ComponentUpdate();

  @Override
  public void reduce(Iterator<PactRecord> records, OutputCollectorV2 out)
      throws Exception {
    PactRecord rec = new PactRecord();
    PactLong number = new PactLong();

    long newCid = Long.MAX_VALUE;
    while (records.hasNext()) {
      rec = records.next();
      number = rec.getField(1, number);

      if (number.getValue() < newCid) {
        newCid = number.getValue();
      }
    }

    result.setCid(newCid);
    result.setVid(rec.getField(0, number).getValue());
    out.collect(result);
  }

  @Override
  public PactRecordIterator getInputIterator(IterationIterator iterationIter) {
    return new PactRecordIterator(iterationIter) {
      ComponentUpdate update = new ComponentUpdate();

      PactLong vid = new PactLong();
      PactLong cid = new PactLong();

      @Override
      public boolean nextPactRecord(MutableObjectIterator<Value> iter,
          PactRecord target) throws IOException {
        boolean success = iter.next(update);

        if (success) {
          vid.setValue(update.getVid());
          cid.setValue(update.getCid());
          target.setField(0, vid);
          target.setField(1, cid);
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
