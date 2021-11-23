package org.apache.flink.mongodb.streaming.source.split;

import java.util.List;

/**
 * MongoSplitStrategy defines how to partition a Mongo data set into {@link MongoSplit}s.
 **/
public interface MongoSplitStrategy {

    List<MongoSplit> split();

}
