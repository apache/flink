package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ClusterPartitionDescriptor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.AbstractID;

public class ClusterPartitionDescriptorImpl implements ClusterPartitionDescriptor {
    private ShuffleDescriptor shuffleDescriptor;
	private final int numberOfSubpartitions;
	private final ResultPartitionType partitionType;
	private final IntermediateDataSetID intermediateDataSetID;

    public ClusterPartitionDescriptorImpl(ShuffleDescriptor shuffleDescriptor,
										  int numberOfSubpartitions,
										  ResultPartitionType partitionType,
										  IntermediateDataSetID intermediateDataSetID) {
        this.shuffleDescriptor = shuffleDescriptor;
		this.numberOfSubpartitions = numberOfSubpartitions;
		this.partitionType = partitionType;
		this.intermediateDataSetID = intermediateDataSetID;
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

	public ResultPartitionType getPartitionType() {
		return partitionType;
	}

	public AbstractID getIntermediateDataSetID() {
		return intermediateDataSetID;
	}
}
