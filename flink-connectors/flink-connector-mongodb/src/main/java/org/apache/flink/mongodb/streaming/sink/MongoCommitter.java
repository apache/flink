package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.mongodb.streaming.connection.MongoClientProvider;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * Committer implementation for {@link MongoSink}
 *
 * <p>The committer is responsible to finalize the Mongo transactions by committing them.
 */
class MongoCommitter implements Committer<DocumentBulk>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MongoCommitter.class);

    @Nullable
    private MongoClient client;

    private final MongoCollection<Document> collection;

    public MongoCommitter(MongoClientProvider clientProvider) {
        this.client = clientProvider.getClient();
        this.collection = clientProvider.getDefaultCollection();
    }

    @Override
    public List<DocumentBulk> commit(List<DocumentBulk> committables) {
        ClientSession session = client.startSession();
        List<DocumentBulk> failedBulk = new ArrayList<>();
        for (DocumentBulk bulk : committables) {
            if (bulk.getDocuments().size() > 0) {
                try {
                    session.startTransaction();
                    collection.insertMany(bulk.getDocuments());
                    session.commitTransaction();
                } catch (Exception e) {
                    LOG.error("Failed to commit with Mongo transaction", e);
                    failedBulk.add(bulk);
                } finally {
                    session.close();
                }
            }
        }
        return failedBulk;
    }

    @Override
    public void close(){
        if (client != null){
            client.close();
        }
    }
}
