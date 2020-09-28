package org.apache.flink.runtime.io.network.partition;

import static org.apache.flink.util.Preconditions.checkState;

public class PipelinedApproximateSubpartitionView extends PipelinedSubpartitionView {

	PipelinedApproximateSubpartitionView(PipelinedApproximateSubpartition parent, BufferAvailabilityListener listener) {
		super(parent, listener);
	}

	@Override
	public void releaseAllResources() {
		if (isReleased.compareAndSet(false, true)) {
			// The view doesn't hold any resources and the parent cannot be restarted. Therefore,
			// it's OK to notify about consumption as well.
			checkState(parent instanceof PipelinedApproximateSubpartition);
			((PipelinedApproximateSubpartition) parent).releaseView();
		}
	}

	@Override
	public String toString() {
		return String.format("PipelinedApproximateSubpartition(index: %d) of ResultPartition %s",
			parent.getSubPartitionIndex(),
			parent.parent.getPartitionId());
	}
}
