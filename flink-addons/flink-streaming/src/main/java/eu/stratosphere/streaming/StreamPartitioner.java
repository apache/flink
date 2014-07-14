package eu.stratosphere.streaming;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.ChannelSelector;

public class StreamPartitioner implements ChannelSelector<StringRecord> {

	@Override
	public int[] selectChannels(StringRecord record, int numberOfOutputChannels) {
		// TODO Auto-generated method stub
		int cellId = Integer.parseInt(record.toString().split(" ")[0]);
		
		return new int[]{cellId % numberOfOutputChannels};
	}

}
