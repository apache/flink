package eu.stratosphere.streaming;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;

public class StreamPartitioner implements ChannelSelector<Record> {

	@Override
	public int[] selectChannels(Record record, int numberOfOutputChannels) {
		IntValue value = new IntValue();
		record.getFieldInto(0, value);
		int cellId = value.getValue();
		return new int[]{cellId % numberOfOutputChannels};
	}
}
