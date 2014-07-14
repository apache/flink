package eu.stratosphere.streaming;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.io.ChannelSelector;

public class DefaultPartitioner  implements ChannelSelector<IOReadableWritable> {

  @Override
  public int[] selectChannels(IOReadableWritable record, int numberOfOutputChannels) {

    int[] returnChannels = new int[numberOfOutputChannels];
    for(int i = 0; i < numberOfOutputChannels;i++) {
      returnChannels[i]=i;
    }
    return returnChannels;
  }

}
