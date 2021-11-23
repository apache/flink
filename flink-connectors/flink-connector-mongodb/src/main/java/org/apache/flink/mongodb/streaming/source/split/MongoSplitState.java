package org.apache.flink.mongodb.streaming.source.split;

/**
 * The mutable version of mongo split.
 **/
public class MongoSplitState extends MongoSplit {

    private long currentOffset;

    public MongoSplitState(MongoSplit mongoSplit) {
        super(mongoSplit.splitId(), mongoSplit.getQuery(), mongoSplit.getStartOffset());
        this.currentOffset = mongoSplit.getStartOffset();
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void increaseOffset(long n) {
        currentOffset += n;
    }
}
