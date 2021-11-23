package org.apache.flink.mongodb.streaming.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.mongodb.connection.MongoClientProvider;
import org.apache.flink.mongodb.streaming.source.split.MongoSplit;
import org.apache.flink.mongodb.streaming.source.split.MongoSplitStrategy;

import com.mongodb.MongoNamespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MongoSplitEnumerator implements SplitEnumerator<MongoSplit, List<MongoSplit>> {

    private final SplitEnumeratorContext<MongoSplit> context;

    private final MongoClientProvider clientProvider;

    private MongoSplitStrategy strategy;

    private final List<MongoSplit> pendingSplits = new ArrayList<>();

    private static final Logger LOG = LoggerFactory.getLogger(MongoSplitEnumerator.class);

    public MongoSplitEnumerator(
            SplitEnumeratorContext<MongoSplit> context,
            MongoClientProvider clientProvider,
            MongoSplitStrategy strategy) {
        this(context, clientProvider, strategy, Collections.emptyList());
    }

    public MongoSplitEnumerator(
            SplitEnumeratorContext<MongoSplit> context,
            MongoClientProvider clientProvider,
            MongoSplitStrategy strategy,
            List<MongoSplit> splits) {
        this.context = context;
        this.clientProvider = clientProvider;
        this.strategy = strategy;
        this.pendingSplits.addAll(splits);
    }

    @Override
    public void start() {
        LOG.info("Starting MongoSplitEnumerator.");
        pendingSplits.addAll(strategy.split());
        MongoNamespace namespace = clientProvider.getDefaultCollection().getNamespace();
        LOG.info("Added {} pending splits for namespace {}.",
                pendingSplits.size(), namespace.getFullName());
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.info("Received split request from task {} on host {}.", subtaskId, requesterHostname);
        if (pendingSplits.size() > 0) {
            MongoSplit nextSplit = pendingSplits.remove(0);
            context.assignSplit(nextSplit, subtaskId);
            LOG.info("Assigned split {} to subtask {}, remaining splits: {}.",
                    nextSplit.splitId(),
                    subtaskId,
                    pendingSplits.size());
        } else {
            LOG.info("No more splits can be assign, signal subtask {}.", subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<MongoSplit> splits, int subtaskId) {
        if (splits != null) {
            LOG.info("Received {} split(s) back from subtask {}.", splits.size(), subtaskId);
            pendingSplits.addAll(splits);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // only add splits if the reader requests
    }

    @Override
    public List<MongoSplit> snapshotState(long checkpointId) throws Exception {
        return pendingSplits;
    }

    @Override
    public void close() throws IOException {
    }
}
