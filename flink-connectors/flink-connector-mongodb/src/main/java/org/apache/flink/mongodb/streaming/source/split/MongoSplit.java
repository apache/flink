package org.apache.flink.mongodb.streaming.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import org.bson.BsonDocument;

/**
 * MongoSplit is composed a query and a start offset.
 **/
public class MongoSplit implements SourceSplit {

    private final String splitId;

    private final BsonDocument query;

    private long startOffset;

    public MongoSplit(String splitId, BsonDocument query) {
        this(splitId, query, 0);
    }

    public MongoSplit(String splitId, BsonDocument query, long startOffset) {
        this.splitId = splitId;
        this.query = query;
        this.startOffset = startOffset;
    }

    public BsonDocument getQuery() {
        return query;
    }

    public long getStartOffset() {
        return startOffset;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
