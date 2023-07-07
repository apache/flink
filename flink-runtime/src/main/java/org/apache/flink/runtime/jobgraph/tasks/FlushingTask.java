package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.runtime.io.network.api.FlushEvent;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface FlushingTask {
    /**
     * This method is called when a task receives a flush event with higher id from one of the input
     * channels.
     * @param flushEvent The flush event received from upstream tasks.
     */
    void triggerFlushEventOnEvent(FlushEvent flushEvent) throws IOException;

    /**
     * This method is used to broadcast flush events to downstream operators, asynchronously
     * by the checkpoint coordinator.
     *
     * <p>This method is called for tasks that start the flushing operation by injecting the initial
     * flush events, i.e., the source tasks. In contrast, flush events on downstream tasks, triggered
     * by receiving flush events, invoke the {@link #triggerFlushEventOnEvent(FlushEvent)} method.
     * */
    CompletableFuture<Boolean> triggerFlushEventAsync(
            long flushEventID, long flushEventTimeStamp);
}
