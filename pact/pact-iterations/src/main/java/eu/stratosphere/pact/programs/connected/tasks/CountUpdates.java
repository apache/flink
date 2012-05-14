package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.programs.connected.types.TransitiveClosureEntry;

public class CountUpdates extends AbstractIterativeTask {
  TransitiveClosureEntry tc = new TransitiveClosureEntry();
  PactRecord result = new PactRecord();
  PactLong count = new PactLong();

  @Override
  public void runIteration(IterationIterator iterationIter) throws Exception {
    int counter = 0;

    while (iterationIter.next(tc)) {
      counter++;
    }

    count.setValue(counter);
    result.setField(0, count);
    output.collect(result);
  }

  @Override
  protected void initTask() {
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

}
