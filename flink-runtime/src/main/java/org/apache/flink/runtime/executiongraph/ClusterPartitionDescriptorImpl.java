package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ClusterPartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

public class ClusterPartitionDescriptorImpl implements ClusterPartitionDescriptor {
    private ShuffleDescriptor shuffleDescriptor;
	private final int numberOfSubpartitions;

    public ClusterPartitionDescriptorImpl(ShuffleDescriptor shuffleDescriptor,
										  int numberOfSubpartitions) {
        this.shuffleDescriptor = shuffleDescriptor;
		this.numberOfSubpartitions = numberOfSubpartitions;
	}

    public ShuffleDescriptor getShuffleDescriptor() {
        return shuffleDescriptor;
    }

    public void setShuffleDescriptor(ShuffleDescriptor shuffleDescriptor) {
        this.shuffleDescriptor = shuffleDescriptor;
    }

	public int getNumberOfSubpartitions() {
		return numberOfSubpartitions;
	}
}
