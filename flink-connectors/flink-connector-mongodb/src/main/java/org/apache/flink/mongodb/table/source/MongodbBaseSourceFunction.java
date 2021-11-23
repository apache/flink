package org.apache.flink.mongodb.table.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mongodb.connection.MongoClientProvider;
import org.apache.flink.mongodb.connection.MongoColloctionProviders;
import org.apache.flink.mongodb.table.sink.MongodbSinkConf;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class MongodbBaseSourceFunction<RowData> extends RichParallelSourceFunction<RowData> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MongodbBaseSourceFunction.class);
    private final MongodbSinkConf mongodbSinkConf;
    private transient MongoClient client;
    private transient List<byte[]> batch;
    private final MongoClientProvider clientProvider;
    private DeserializationSchema<RowData> deserializer;

    protected MongodbBaseSourceFunction(
            MongodbSinkConf mongodbSinkConf,
            DeserializationSchema<RowData> deserializer) {
        this.mongodbSinkConf = mongodbSinkConf;
        this.deserializer = deserializer;
        this.clientProvider = MongoColloctionProviders
                .getBuilder()
                .setServers(this.mongodbSinkConf.getUri())
                .setdatabase(this.mongodbSinkConf.getDatabase())
                .setCollection(this.mongodbSinkConf.getCollection()).build();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.client = clientProvider.getClient();
        this.batch = new ArrayList();
    }

    @Override
    public void close() throws Exception {
        read();
        super.close();
        this.client.close();
        this.client = null;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        read();
        batch.forEach(new Consumer<byte[]>() {
            @Override
            public void accept(byte[] bytes) {
                try {
                    ctx.collect(deserializer.deserialize(bytes));
                } catch (IOException e) {
                    LOG.info(e.getMessage());
                }
            }
        });

    }

    @Override
    public void cancel() {

    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws IOException {
        read();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {
    }

    private void read() throws IOException {

        MongoDatabase mongoDatabase = this.client.getDatabase(this.mongodbSinkConf.getDatabase());

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(this.mongodbSinkConf
                .getCollection());
        FindIterable<Document> documents = mongoCollection.find();
        MongoCursor<Document> iterator = documents.iterator();
        while (iterator.hasNext()) {
            batch.add(iterator.next().toJson().getBytes(StandardCharsets.UTF_8));
        }
    }

}
