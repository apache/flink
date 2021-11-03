package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;

@Internal
public class CheckpointSummary implements SinkMessage {
    final int subtaskId;
    /** May change after recovery. */
    final int numberOfSubtasks;

    final long checkpointId;
    final int numberCommittablesOfSubtask;

    public CheckpointSummary(
            int subtaskId,
            int numberOfSubtasks,
            long checkpointId,
            int numberCommittablesOfSubtask) {
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
        this.checkpointId = checkpointId;
        this.numberCommittablesOfSubtask = numberCommittablesOfSubtask;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public int getNumberCommittablesOfSubtask() {
        return numberCommittablesOfSubtask;
    }
}
