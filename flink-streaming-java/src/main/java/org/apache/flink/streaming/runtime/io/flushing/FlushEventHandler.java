package org.apache.flink.streaming.runtime.io.flushing;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.FlushEvent;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.Closeable;
import java.io.IOException;

/**
 * The {@link FlushEventHandler} keeps track of the latest flush event received by a task. It would
 * only broadcast flush events to down stream operators on receiving a flush event with greater id.
 */
public class FlushEventHandler implements Closeable {

    private final CheckpointableTask toNotifyOnFlushEvent;

    private final String taskName;

    /** The highest flush event ID encountered so far. */
    private long lastFlushingEventID = -1;

    public FlushEventHandler(CheckpointableTask toNotifyOnFlushEvent, String taskName) {
        this.toNotifyOnFlushEvent = toNotifyOnFlushEvent;
        this.taskName = taskName;
    }

    /**
     * Invoked when a task receives flush event from input channels.
     * @param flushEvent The flush event from upstream tasks.
     * @param channelInfo of which the flush event is received.
     */
    public void processFlushEvent(
            FlushEvent flushEvent, InputChannelInfo channelInfo)
            throws IOException {
        if (flushEvent.getFlushEventId() > lastFlushingEventID) {
            lastFlushingEventID = flushEvent.getFlushEventId();
            toNotifyOnFlushEvent.triggerFlushEventOnEvent(flushEvent);
        }
    }

    @Override
    public void close() throws IOException {}
}
