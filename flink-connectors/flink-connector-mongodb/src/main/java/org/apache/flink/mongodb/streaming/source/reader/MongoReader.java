package org.apache.flink.mongodb.streaming.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.mongodb.connection.MongoClientProvider;
import org.apache.flink.mongodb.streaming.serde.DocumentDeserializer;
import org.apache.flink.mongodb.streaming.source.split.MongoSplit;
import org.apache.flink.mongodb.streaming.source.split.MongoSplitState;

import org.bson.Document;

import java.util.Map;

public class MongoReader<E> extends SingleThreadMultiplexSourceReaderBase<Document, E, MongoSplit, MongoSplitState> {

    public MongoReader(
            SourceReaderContext context,
            MongoClientProvider clientProvider,
            DocumentDeserializer<E> deserializer) {
        super(
                () -> new MongoSplitReader(clientProvider),
                new MongoEmitter<>(deserializer),
                context.getConfiguration(),
                context
        );
    }

    @Override
    public void start() {
        context.sendSplitRequest();
    }

    @Override
    protected MongoSplitState initializedState(MongoSplit split) {
        return new MongoSplitState(split);
    }

    @Override
    protected MongoSplit toSplitType(String splitId, MongoSplitState splitState) {
        return new MongoSplit(splitId, splitState.getQuery(), splitState.getCurrentOffset());
    }

    @Override
    protected void onSplitFinished(Map<String, MongoSplitState> map) {
        context.sendSplitRequest();
    }
}
