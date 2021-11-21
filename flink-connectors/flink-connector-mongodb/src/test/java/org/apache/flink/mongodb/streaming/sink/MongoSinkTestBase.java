package org.apache.flink.mongodb.streaming.sink;


import org.apache.flink.mongodb.streaming.EmbeddedMongoTestBase;
import org.apache.flink.mongodb.streaming.serde.DocumentSerializer;

import org.bson.Document;

/**
 * Base class for tests for MongoSink.
 **/

public class MongoSinkTestBase extends EmbeddedMongoTestBase {

    protected static String DATABASE_NAME = "bulkwrite";
    protected static String COLLECTION = "transactional-4_0";

}


class StringDocumentSerializer implements DocumentSerializer<String> {

    @Override
    public Document serialize(String string) {
        Document document = new Document();
        String[] elements = string.split(",");
        document.append("_id", elements[0]);
        document.append("count", Integer.parseInt(elements[1]));
        return document;
    }
}

