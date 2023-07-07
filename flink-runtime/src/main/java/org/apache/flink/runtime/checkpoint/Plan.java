package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.executiongraph.Execution;

import java.util.List;

public interface Plan {
    /** Returns the tasks who need to be sent a message when a checkpoint is started. */
    List<Execution> getTasksToTrigger();
}
