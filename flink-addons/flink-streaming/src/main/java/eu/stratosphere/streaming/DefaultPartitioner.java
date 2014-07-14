package eu.stratosphere.streaming;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.types.Record;

public class DefaultPartitioner  implements ChannelSelector<Record> {

  @Override
  public int[] selectChannels(Record record, int numberOfOutputChannels) {

    int[] returnChannels = new int[numberOfOutputChannels];
    for(int i = 0; i < numberOfOutputChannels;i++) {
      returnChannels[i]=i;
    }
    return returnChannels;
  }

}
