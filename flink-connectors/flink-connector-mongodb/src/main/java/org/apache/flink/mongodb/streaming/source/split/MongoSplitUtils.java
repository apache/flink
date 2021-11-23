package org.apache.flink.mongodb.streaming.source.split;

import org.bson.BsonDocument;

import javax.annotation.Nullable;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;

/**
 * Helper class for using {@link MongoSplit}.
 **/
public class MongoSplitUtils {

    private static final String SPLIT_ID_TEMPLATE = "split-%d";

    public static MongoSplit createMongoSplit(
            int index,
            BsonDocument matchQuery,
            String splitKey,
            @Nullable Object lowerBound,
            @Nullable Object upperBound) {
        return createMongoSplit(index, matchQuery, splitKey, lowerBound, upperBound, 0);
    }

    public static MongoSplit createMongoSplit(
            int index,
            BsonDocument matchQuery,
            String splitKey,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            long startOffset) {
        BsonDocument splitQuery = new BsonDocument();
        if (matchQuery != null) {
            matchQuery.forEach(splitQuery::append);
        }
        if (splitKey != null) {
            BsonDocument boundaryQuery;
            if (lowerBound != null && upperBound != null) {
                boundaryQuery = and(gte(splitKey, lowerBound), lt(splitKey, upperBound))
                        .toBsonDocument();
            } else if (lowerBound != null) {
                boundaryQuery = gte(splitKey, lowerBound).toBsonDocument();
            } else if (upperBound != null) {
                boundaryQuery = lt(splitKey, upperBound).toBsonDocument();
            } else {
                boundaryQuery = new BsonDocument();
            }
            boundaryQuery.forEach(
                    splitQuery::append
            );
        }
        return new MongoSplit(String.format(SPLIT_ID_TEMPLATE, index), splitQuery, startOffset);
    }
}
