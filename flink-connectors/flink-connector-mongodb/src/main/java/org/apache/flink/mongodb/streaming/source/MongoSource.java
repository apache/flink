package org.apache.flink.mongodb.streaming.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.mongodb.connection.MongoClientProvider;
import org.apache.flink.mongodb.streaming.serde.DocumentDeserializer;
import org.apache.flink.mongodb.streaming.source.enumerator.MongoSplitEnumerator;
import org.apache.flink.mongodb.streaming.source.reader.MongoReader;
import org.apache.flink.mongodb.streaming.source.split.ListMongoSplitSerializer;
import org.apache.flink.mongodb.streaming.source.split.MongoSplit;
import org.apache.flink.mongodb.streaming.source.split.MongoSplitSerializer;
import org.apache.flink.mongodb.streaming.source.split.MongoSplitStrategy;

import java.util.List;

public class MongoSource<T> implements Source<T, MongoSplit, List<MongoSplit>> {

    private MongoClientProvider clientProvider;

    private DocumentDeserializer<T> deserializer;

    private MongoSplitStrategy splitStrategy;

    public MongoSource(
            MongoClientProvider clientProvider,
            DocumentDeserializer<T> deserializer,
            MongoSplitStrategy splitStrategy) {
        this.clientProvider = clientProvider;
        this.deserializer = deserializer;
        this.splitStrategy = splitStrategy;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, MongoSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new MongoReader<>(readerContext, clientProvider, deserializer);
    }

    @Override
    public SplitEnumerator<MongoSplit, List<MongoSplit>> createEnumerator(
            SplitEnumeratorContext<MongoSplit> enumContext) throws Exception {
        return new MongoSplitEnumerator(enumContext, clientProvider, splitStrategy);
    }

    @Override
    public SplitEnumerator<MongoSplit, List<MongoSplit>> restoreEnumerator(
            SplitEnumeratorContext<MongoSplit> enumContext,
            List<MongoSplit> checkpointedSplits) throws Exception {
        return new MongoSplitEnumerator(
                enumContext,
                clientProvider,
                splitStrategy,
                checkpointedSplits);
    }

    @Override
    public SimpleVersionedSerializer<MongoSplit> getSplitSerializer() {
        return MongoSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<List<MongoSplit>> getEnumeratorCheckpointSerializer() {
        return ListMongoSplitSerializer.INSTANCE;
    }
}
