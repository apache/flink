package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.mongodb.connection.MongoClientProvider;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MongoCommitter flushes data to MongoDB in a transaction. Due to MVCC implementation of MongoDB, a transaction is
 * not recommended to be large.
 **/
public class MongoCommitter implements Committer<DocumentBulk>, Closeable {

    private final MongoClient client;

    private final MongoCollection<Document> collection;

    private final static Logger LOGGER = LoggerFactory.getLogger(MongoCommitter.class);

    private TransactionOptions txnOptions = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY)
            .build();

    public MongoCommitter(MongoClientProvider clientProvider) {
        this.client = clientProvider.getClient();
        this.collection = clientProvider.getDefaultCollection();
    }

    @Override
    public List<DocumentBulk> commit(List<DocumentBulk> committables) throws IOException {
        ClientSession session = client.startSession();
        List<DocumentBulk> failedBulk = new ArrayList<>();
        for (DocumentBulk bulk : committables) {
            if (bulk.getDocuments().size() > 0) {
                CommittableTransaction transaction = new CommittableTransaction(collection, bulk.getDocuments());
                try {
                    session.withTransaction(transaction, txnOptions);
                } catch (Exception e) {
                    LOGGER.error("Failed to commit with Mongo transaction", e);
                    failedBulk.add(bulk);
                    session.close();
                }
            }
        }
        return failedBulk;
    }

    @Override
    public void close() {
        client.close();
    }
}
