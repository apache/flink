package org.apache.flink.api.connector.source.lib;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.util.GeneratorSequenceIterator;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A split of the source, representing a number sub-sequence. */
public class NumberSequenceSplit<OUT>
        implements IteratorSourceSplit<OUT, GeneratorSequenceIterator<OUT>> {

    private final String splitId;
    private final long from;
    private final long to;

    private final MapFunction<Long, OUT> mapFunction;

    public NumberSequenceSplit(
            String splitId, long from, long to, MapFunction<Long, OUT> mapFunction) {
        checkArgument(from <= to, "'from' must be <= 'to'");
        this.splitId = checkNotNull(splitId);
        this.mapFunction = checkNotNull(mapFunction);
        this.from = from;
        this.to = to;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public long from() {
        return from;
    }

    public long to() {
        return to;
    }

    public MapFunction<Long, OUT> mapFunction() {
        return mapFunction;
    }

    @Override
    public GeneratorSequenceIterator<OUT> getIterator() {
        return GeneratorSequenceIterator.of(from, to, mapFunction);
    }

    @Override
    public IteratorSourceSplit<OUT, GeneratorSequenceIterator<OUT>> getUpdatedSplitForIterator(
            GeneratorSequenceIterator<OUT> iterator) {
        return new NumberSequenceSplit<>(
                splitId, iterator.getCurrent(), iterator.getTo(), mapFunction);
    }

    @Override
    public String toString() {
        return String.format("NumberSequenceSplit [%d, %d] (%s)", from, to, splitId);
    }
}
