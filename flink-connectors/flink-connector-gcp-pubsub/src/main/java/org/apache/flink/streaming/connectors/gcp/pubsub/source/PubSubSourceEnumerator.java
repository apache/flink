package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** */
public class PubSubSourceEnumerator
        implements SplitEnumerator<PubSubSplit, PubSubEnumeratorCheckpoint> {
    private final SplitEnumeratorContext<PubSubSplit> context;

    public PubSubSourceEnumerator(SplitEnumeratorContext<PubSubSplit> context) {
        this.context = context;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addSplitsBack(List<PubSubSplit> splits, int subtaskId) {}

    @Override
    public void addReader(int subtaskId) {
        context.assignSplit(new PubSubSplit(), subtaskId);
    }

    @Override
    public PubSubEnumeratorCheckpoint snapshotState() throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
