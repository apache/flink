package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;

/**
 * Default wrapper implementation that uses an {@link ArrayDeque} as the underlying data structure.
 */
@PublicEvolving
public class DequeBufferWrapper<RequestEntryT extends Serializable>
        implements BufferWrapper<RequestEntryT> {

    private final Deque<RequestEntryWrapper<RequestEntryT>> buffer;
    private long totalSizeInBytes;

    /** Creates an empty buffer backed by an {@link ArrayDeque}. */
    private DequeBufferWrapper() {
        buffer = new ArrayDeque<>();
        totalSizeInBytes = 0L;
    }

    /**
     * Adds a request entry to the buffer. If {@code prioritize} is true, the entry is inserted at
     * the front (for retries). Otherwise, it is added at the end following FIFO order.
     *
     * @param entry The request entry to add.
     * @param prioritize If true, insert at the front; otherwise, add at the end.
     */
    @Override
    public void add(RequestEntryWrapper<RequestEntryT> entry, boolean prioritize) {
        if (prioritize) {
            buffer.addFirst(entry);
        } else {
            buffer.add(entry);
        }
        totalSizeInBytes += entry.getSize();
    }

    /** {@inheritDoc} */
    @Override
    public RequestEntryWrapper<RequestEntryT> peek() {
        return buffer.peek();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return buffer.size();
    }

    /** {@inheritDoc} */
    @Override
    public Collection<RequestEntryWrapper<RequestEntryT>> getBufferedState() {
        return new ArrayList<>(buffer);
    }

    /** {@inheritDoc} */
    @Override
    public RequestEntryWrapper<RequestEntryT> poll() {
        RequestEntryWrapper<RequestEntryT> entry = buffer.poll();
        if (entry != null) {
            totalSizeInBytes = Math.max(0, totalSizeInBytes - entry.getSize());
        }
        return entry;
    }

    /** {@inheritDoc} */
    @Override
    public long totalSizeInBytes() {
        return totalSizeInBytes;
    }

    /**
     * Builder for {@link DequeBufferWrapper}.
     *
     * @param <RequestEntryT> The type of request entries that the buffer wrapper will store.
     */
    public static class Builder<RequestEntryT extends Serializable>
            implements BufferWrapper.Builder<DequeBufferWrapper<RequestEntryT>, RequestEntryT> {

        @Override
        public DequeBufferWrapper<RequestEntryT> build() {
            return new DequeBufferWrapper<>();
        }
    }
}
