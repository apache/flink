package org.apache.flink.state.api.runtime.metadata;

import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;

import java.util.Collection;
import java.util.Collections;

/**
 * Returns metadata for a new savepoint.
 */
public class NewSavepointMetadata implements SavepointMetadata {

	private final int maxParallelism;

	public NewSavepointMetadata(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}

	@Override
	public int maxParallelism() {
		return maxParallelism;
	}

	@Override
	public Collection<MasterState> getMasterStates() {
		return Collections.emptyList();
	}

	@Override
	public Collection<OperatorState> getOperatorStates() {
		return Collections.emptyList();
	}
}
