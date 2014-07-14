package eu.stratosphere.streaming;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class StreamPartitioner implements ChannelSelector<Record> {

	/*@Override
	public int[] selectChannels(StringRecord record, int numberOfOutputChannels) {
		int cellId = Integer.parseInt(record.toString().split(" ")[0]);		
		return new int[]{cellId % numberOfOutputChannels};
	}*/

	@Override
	public int[] selectChannels(Record record, int numberOfOutputChannels) {
		StringValue value = new StringValue("");
		record.getFieldInto(0, value);
		int cellId = Integer.parseInt(value.getValue().split(" ")[0]);
		return new int[]{cellId % numberOfOutputChannels};
	}
}
