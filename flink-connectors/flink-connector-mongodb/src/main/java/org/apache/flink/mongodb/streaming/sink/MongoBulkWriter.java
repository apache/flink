package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.mongodb.streaming.serde.DocumentSerializer;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MongoBulkWriter<IN> implements SinkWriter<IN, DocumentBulk, DocumentBulk> {

    private transient MongoCollection<Document> collection;

    private DocumentBulk currentBulk;

    private final List<DocumentBulk> pendingBulks = new ArrayList<>();

    private DocumentSerializer<IN> serializer;

    private transient ScheduledExecutorService scheduler;

    private transient ScheduledFuture scheduledFuture;

    private transient volatile Exception flushException;

    private final long maxSize;

    private final boolean flushOnCheckpoint;

    private final RetryPolicy retryPolicy = new RetryPolicy(3, 1000L);

    private transient volatile boolean closed = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoBulkWriter.class);

    public MongoBulkWriter(
            MongoCollection<Document> collection,
            DocumentSerializer<IN> serializer,
            long maxSize,
            long bulkFlushInterval,
            boolean flushOnCheckpoint,
            boolean model
    ) {

        this.serializer = serializer;
        this.maxSize = maxSize;
        this.currentBulk = new DocumentBulk(maxSize);
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.collection = collection;
        this.serializer = serializer;

        if (!flushOnCheckpoint && bulkFlushInterval > 0) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("mongodb-bulk-writer"));
            this.scheduledFuture =
                    scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (MongoBulkWriter.this) {
                                    if (!closed) {
                                        try {
                                            rollBulkIfNeeded(true);
                                            if (model) {
                                                mergeInto();
                                            }
                                            flushInsert();
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            bulkFlushInterval,
                            bulkFlushInterval,
                            TimeUnit.MILLISECONDS);
        }
    }

    public void initializeState(List<DocumentBulk> recoveredBulks) {

        for (DocumentBulk bulk : recoveredBulks) {
            for (Document document : bulk.getDocuments()) {
                rollBulkIfNeeded();
                currentBulk.add(document);
            }
        }
    }

    @Override
    public void write(IN o, Context context) throws IOException {
        checkFlushException();
        rollBulkIfNeeded();
        currentBulk.add(serializer.serialize(o));
    }

    @Override
    public List<DocumentBulk> prepareCommit(boolean flush) throws IOException {
        if (flushOnCheckpoint || flush) {
            rollBulkIfNeeded(true);
        }
        return pendingBulks;
    }

    @Override
    public List<DocumentBulk> snapshotState() throws IOException {
        List<DocumentBulk> inProgressAndPendingBulks = new ArrayList<>(1);
        inProgressAndPendingBulks.add(currentBulk);
        inProgressAndPendingBulks.addAll(pendingBulks);
        pendingBulks.clear();
        return inProgressAndPendingBulks;
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    /**
     * Update exists, no insert exists, note: the primary key ID is required to be unique
     */
    private synchronized void mergeInto() {
        if (!closed) {
            ensureConnection();
            retryPolicy.reset();
            Iterator<DocumentBulk> iterator = pendingBulks.iterator();
            while (iterator.hasNext()) {
                DocumentBulk bulk = iterator.next();
                do {
                    try {
                        List<Document> documents = bulk.getDocuments();
                        if (documents.size() == 0) {
                            break;
                        }

                        List<WriteModel<Document>> batchOperateList = new ArrayList<>();
                        documents.forEach(new Consumer<Document>() {
                            @Override
                            public void accept(Document document) {
                                Bson filter = Filters.eq("_id", document.get("_id"));
                                UpdateOptions options = new UpdateOptions().upsert(true);
                                UpdateOneModel<Document> updateOneModel = new UpdateOneModel<>(
                                        filter,
                                        new Document("$set", document),
                                        options);
                                batchOperateList.add(updateOneModel);
                            }
                        });
                        BulkWriteOptions options = new BulkWriteOptions();
                        options.ordered(false);
                        BulkWriteResult bulkWriteResult = collection.bulkWrite(
                                batchOperateList,
                                options);
                        iterator.remove();
                        break;
                    } catch (MongoException e) {
                        LOGGER.error("Failed to mergeInto data to MongoDB:{}", e.getMessage());
                    }
                } while (!closed && retryPolicy.shouldBackoffRetry());
            }
        }
    }

    private synchronized void flushInsert() {
        if (!closed) {
            ensureConnection();
            retryPolicy.reset();
            Iterator<DocumentBulk> iterator = pendingBulks.iterator();
            while (iterator.hasNext()) {
                DocumentBulk bulk = iterator.next();
                do {
                    try {
                        // ordered, non-bypass mode
                        collection.insertMany(bulk.getDocuments());
                        iterator.remove();
                        break;
                    } catch (MongoException e) {
                        // maybe partial failure
                        LOGGER.error("Failed to flushInsert data to MongoDB:{}", e.getMessage());
                    }
                } while (!closed && retryPolicy.shouldBackoffRetry());
            }
        }
    }

    private void ensureConnection() {
        try {
            collection.listIndexes();
        } catch (MongoException e) {
            LOGGER.warn("MongdbOP Connection is not available, try to reconnect", e);
        }
    }

    private void rollBulkIfNeeded() {
        rollBulkIfNeeded(false);
    }

    private synchronized void rollBulkIfNeeded(boolean force) {
        if (force || currentBulk.isFull()) {
            pendingBulks.add(currentBulk);
            currentBulk = new DocumentBulk(maxSize);
        }
    }

    private void checkFlushException() throws IOException {
        if (flushException != null) {
            throw new IOException("Failed to flush records to MongoDB", flushException);
        }
    }

    @NotThreadSafe
    class RetryPolicy {

        private final long maxRetries;

        private final long backoffMillis;

        private long currentRetries = 0L;

        RetryPolicy(long maxRetries, long backoffMillis) {
            this.maxRetries = maxRetries;
            this.backoffMillis = backoffMillis;
        }

        boolean shouldBackoffRetry() {
            if (++currentRetries > maxRetries) {
                return false;
            } else {
                backoff();
                return true;
            }
        }

        private void backoff() {
            try {
                Thread.sleep(backoffMillis);
            } catch (InterruptedException e) {
            }
        }

        void reset() {
            currentRetries = 0L;
        }
    }
}
