package eu.stratosphere.pact.programs.preparation.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;

public class Longify extends AbstractMinimalTask {
  PactRecord record = new PactRecord();
  PactRecord result = new PactRecord();

  PactString key = new PactString();
  PactString value = new PactString();
  PactLong newKey = new PactLong();
  PactLong newValue = new PactLong();


  @Override
  protected void initTask() {
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

  @Override
  public void run() throws Exception {
    MutableObjectIterator<Value> input = inputs[0];
    while (input.next(record)) {
      key = record.getField(0, key);
      value = record.getField(1, value);
      newKey.setValue(key.getValue().hashCode());
      newValue.setValue(value.getValue().hashCode());
      result.setField(0, newKey);
      result.setField(1, newValue);
      output.collect(result);
    }
  }

}
