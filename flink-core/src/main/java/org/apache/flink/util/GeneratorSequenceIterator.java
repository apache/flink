package org.apache.flink.util;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.MapFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Public
public class GeneratorSequenceIterator<OUT> extends NewSplittableIterator<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(GeneratorSequenceIterator.class);

    private static final long serialVersionUID = 1L;

    private final MapFunction<Long, OUT> generator;
    private final NumberSequenceIterator numSeqIterator;

    public static <OUT> GeneratorSequenceIterator<OUT> of(
            long from, long to, MapFunction<Long, OUT> generator) {
        return new GeneratorSequenceIterator<>(from, to, generator);
    }

    private GeneratorSequenceIterator(long from, long to, MapFunction<Long, OUT> generator) {
        if (from > to) {
            throw new IllegalArgumentException(
                    "The 'to' value must not be smaller than the 'from' value.");
        }
        this.numSeqIterator = new NumberSequenceIterator(from, to);
        this.generator = checkNotNull(generator);
    }

    public long getCurrent() {
        return numSeqIterator.getCurrent();
    }

    public long getTo() {
        return numSeqIterator.getTo();
    }

    @Override
    public boolean hasNext() {
        return numSeqIterator.hasNext();
    }

    @Override
    public OUT next() {
        long nextElementId = numSeqIterator.next();
        try {
            return generator.map(nextElementId);
        } catch (Exception e) {
            LOG.error(
                    "Exception thrown in Generator user function at element {}", nextElementId, e);
            throw new RuntimeException(
                    String.format(
                            "Exception thrown in Generator user function at element %d",
                            nextElementId));
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<? extends Iterator<OUT>> split(int numPartitions) {
        NumberSequenceIterator[] splits = numSeqIterator.split(numPartitions);
        return Arrays.stream(splits)
                .map(
                        s ->
                                new GeneratorSequenceIterator<>(
                                        s.getCurrent(), s.getTo(), this.generator))
                .collect(Collectors.toList());
    }

    @Override
    public int getMaximumNumberOfSplits() {
        return numSeqIterator.getMaximumNumberOfSplits();
    }
}
