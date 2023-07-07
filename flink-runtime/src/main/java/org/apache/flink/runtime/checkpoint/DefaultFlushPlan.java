package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.executiongraph.Execution;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The plan of emitting flush events, indicating which tasks should send flush events.
 */
public class DefaultFlushPlan implements Plan {
    private final List<Execution> tasksToTrigger;

    public DefaultFlushPlan(List<Execution> tasksToTrigger) {
        this.tasksToTrigger = checkNotNull(tasksToTrigger);
    }

    @Override
    public List<Execution> getTasksToTrigger() {
        return tasksToTrigger;
    }
}
