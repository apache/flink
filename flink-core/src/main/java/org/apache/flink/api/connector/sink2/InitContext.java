package org.apache.flink.api.connector.sink2;

import org.apache.flink.api.common.JobID;

import java.util.OptionalLong;

/**
 * Common interface which exposes runtime info for creating {@link SinkWriter} and {@link Committer}
 * objects.
 */
public interface InitContext {
    /**
     * The first checkpoint id when an application is started and not recovered from a previously
     * taken checkpoint or savepoint.
     */
    long INITIAL_CHECKPOINT_ID = 1;

    /** @return The id of task where the committer is running. */
    int getSubtaskId();

    /** @return The number of parallel committer tasks. */
    int getNumberOfParallelSubtasks();

    /**
     * Gets the attempt number of this parallel subtask. First attempt is numbered 0.
     *
     * @return Attempt number of the subtask.
     */
    int getAttemptNumber();

    /**
     * Returns id of the restored checkpoint, if state was restored from the snapshot of a previous
     * execution.
     */
    OptionalLong getRestoredCheckpointId();

    /**
     * The ID of the current job. Note that Job ID can change in particular upon manual restart. The
     * returned ID should NOT be used for any job management tasks.
     */
    JobID getJobId();
}
