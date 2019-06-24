package org.apache.flink.state.api.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;

/**
 * Takes a final snapshot of the state of an operator subtask.
 */
@Internal
public final class SnapshotUtils {
	static final long CHECKPOINT_ID = 0L;

	private SnapshotUtils() {}

	public static <OUT, OP extends StreamOperator<OUT>> TaggedOperatorSubtaskState snapshot(
		OP operator,
		int index,
		long timestamp,
		CheckpointStorageWorkerView checkpointStorage,
		Path savepointPath) throws Exception {

		CheckpointOptions options = new CheckpointOptions(
			CheckpointType.SAVEPOINT,
			AbstractFsCheckpointStorage.encodePathAsReference(savepointPath));

		operator.prepareSnapshotPreBarrier(CHECKPOINT_ID);

		CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(
			CHECKPOINT_ID,
			options.getTargetLocation());

		operator.prepareSnapshotPreBarrier(CHECKPOINT_ID);

		OperatorSnapshotFutures snapshotInProgress = operator.snapshotState(
			CHECKPOINT_ID,
			timestamp,
			options,
			storage);

		OperatorSubtaskState state = new OperatorSnapshotFinalizer(snapshotInProgress).getJobManagerOwnedState();

		operator.notifyCheckpointComplete(CHECKPOINT_ID);
		return new TaggedOperatorSubtaskState(index, state);
	}
}
