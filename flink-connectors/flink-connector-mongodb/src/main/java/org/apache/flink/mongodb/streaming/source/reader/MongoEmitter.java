package org.apache.flink.mongodb.streaming.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.mongodb.streaming.serde.DocumentDeserializer;
import org.apache.flink.mongodb.streaming.source.split.MongoSplitState;

import org.bson.Document;

public class MongoEmitter<E> implements RecordEmitter<Document, E, MongoSplitState> {

    private final DocumentDeserializer<E> deserializer;

    MongoEmitter(DocumentDeserializer<E> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void emitRecord(
            Document element,
            SourceOutput<E> output,
            MongoSplitState splitState) throws Exception {
        output.collect(deserializer.deserialize(element));
        splitState.increaseOffset(1);
    }
}
