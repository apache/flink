package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;


public class FlushEvent extends RuntimeEvent {

    private final long flushEventId;
    private final long timestamp;

    public FlushEvent(long flushEventId, long timestamp) {
        this.flushEventId = flushEventId;
        this.timestamp = timestamp;
    }

    public long getFlushEventId() {
        return flushEventId;
    }

    public long getTimestamp() {
        return timestamp;
    }
    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return (int) (flushEventId ^ (flushEventId >>> 32) ^ timestamp ^ (timestamp >>> 32));
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null || other.getClass() != FlushEvent.class) {
            return false;
        } else {
            FlushEvent that = (FlushEvent) other;
            return that.flushEventId == this.flushEventId
                    && that.timestamp == this.timestamp;
        }
    }

    @Override
    public String toString() {
        return String.format(
                "Flush Event %d @ %d", flushEventId, timestamp);
    }
}
