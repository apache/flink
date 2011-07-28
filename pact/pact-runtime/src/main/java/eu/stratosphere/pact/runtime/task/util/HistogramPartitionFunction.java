package eu.stratosphere.pact.runtime.task.util;

import java.util.Arrays;

import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.type.Key;

public class HistogramPartitionFunction implements PartitionFunction {
	private final Key[] splitBorders;
	private final Order partitionOrder;
	private final int[] channels = new int[1];
	
	public HistogramPartitionFunction(Key[] splitBorders, Order partitionOrder) {
		this.splitBorders = splitBorders;
		this.partitionOrder = partitionOrder;
	}

	@Override
	public int[] selectChannels(Key data, int numChannels) {
		//TODO: Check partition borders match number of channels
		int pos = Arrays.binarySearch(splitBorders, data);
		if(pos < 0) {
			pos++;
			pos = -pos;
		}
		
		if(partitionOrder == Order.ASCENDING || partitionOrder == Order.ANY) {
			channels[0] = pos;
		} else {
			channels[0] = splitBorders.length  - pos;
		}
		
		return channels;
	}
}
