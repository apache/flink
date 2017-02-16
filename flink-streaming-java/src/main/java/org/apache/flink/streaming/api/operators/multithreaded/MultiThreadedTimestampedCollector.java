package org.apache.flink.streaming.api.operators.multithreaded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * A multi-threaded wrapper around an {@link Output} for user functions that expect a {@link Collector}.
 * Before giving the {@link MultiThreadedTimestampedCollector} to a user function you must set
 * the timestamp that should be attached to emitted elements. Most operators
 * would set the timestamp of the incoming
 * {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord} here.
 *
 * @param <T> The type of the elements that can be emitted.
 */
@Internal
public class MultiThreadedTimestampedCollector<T> extends TimestampedCollector<T> {
    private Object lock;

    /**
     * Creates a new {@link MultiThreadedTimestampedCollector} that wraps the given {@link Output}.
     *
     * @param output
     */
    public MultiThreadedTimestampedCollector(Output<StreamRecord<T>> output, Object lock) {
        super(output);
        this.lock = lock;
    }

    @Override
    public void collect(T record) {
        synchronized (lock) {
            output.collect(reuse.replace(record));
        }
    }
}
