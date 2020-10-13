package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.PersistedIntermediateResultDescriptor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.ArrayList;
import java.util.Collection;

public class PersistedIntermediateResultDescriptorImpl implements PersistedIntermediateResultDescriptor {
	private Collection<ClusterPartitionDescriptorImpl> clusterPartitionDescriptors = new ArrayList<>();
	private final IntermediateDataSetID intermediateDataSetID;
	private final ResultPartitionType resultPartitionType;

	public PersistedIntermediateResultDescriptorImpl(IntermediateDataSetID intermediateDataSetID,
													 ResultPartitionType resultPartitionType) {
		this.intermediateDataSetID = intermediateDataSetID;
		this.resultPartitionType = resultPartitionType;
	}

	public Collection<ClusterPartitionDescriptorImpl> getClusterPartitionDescriptors() {
		return clusterPartitionDescriptors;
	}

	public ResultPartitionType getResultPartitionType() {
		return resultPartitionType;
	}

	@Override
	public IntermediateDataSetID getIntermediateDataSetId() {
		return intermediateDataSetID;
	}

	public void addClusterPartitionDescriptor(ClusterPartitionDescriptorImpl clusterPartitionDescriptor) {
		clusterPartitionDescriptors.add(clusterPartitionDescriptor);
	}
}
