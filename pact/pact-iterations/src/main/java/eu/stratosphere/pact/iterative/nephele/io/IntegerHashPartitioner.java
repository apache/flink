package eu.stratosphere.pact.iterative.nephele.io;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.PartitionFunction;

public class IntegerHashPartitioner implements PartitionFunction {

  @Override
  public void selectChannels(PactRecord data, int numChannels, int[] channels) {
    channels[0] = data.getField(0, PactInteger.class).getValue() % numChannels;
  }

}
