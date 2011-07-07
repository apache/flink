package eu.stratosphere.pact.runtime.task.util;

import eu.stratosphere.pact.common.type.Key;

public interface PartitionFunction {
	public int[] selectChannels(Key data, int numChannels);
}
