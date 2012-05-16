package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.programs.connected.types.TransitiveClosureEntry;
import eu.stratosphere.pact.programs.connected.types.TransitiveClosureEntryAccessors;
import eu.stratosphere.pact.programs.preparation.tasks.LongList;

public class ConvertToTransitiveClosureTypes extends AbstractMinimalTask {

  @Override
  protected void initTask() {
    outputAccessors[0] = new TransitiveClosureEntryAccessors();
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

  @Override
  public void run() throws Exception {
    PactRecord record = new PactRecord();
    TransitiveClosureEntry tc = new TransitiveClosureEntry();

    PactLong number = new PactLong();
    LongList neighbours = new LongList();

    while (inputs[0].next(record)) {
      long vid = record.getField(0, number).getValue();
      neighbours = record.getField(1, neighbours);

      tc.setVid(vid);
      tc.setNeighbors(neighbours.getList(), neighbours.getLength());
      output.collect(tc);
    }
  }

}
