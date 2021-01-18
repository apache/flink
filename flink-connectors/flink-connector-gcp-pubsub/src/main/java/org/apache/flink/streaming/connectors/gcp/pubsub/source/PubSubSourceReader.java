package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Map;
import java.util.function.Supplier;

/** */
public class PubSubSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                Tuple2<T, Long>, T, PubSubSplit, PubSubSplitState> {
    public PubSubSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<T, Long>>> elementsQueue,
            Supplier<PubSubSplitReader<T>> splitReaderSupplier,
            RecordEmitter<Tuple2<T, Long>, T, PubSubSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        //		TODO: right super constructor?
        super(
                elementsQueue,
                new SingleThreadFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, PubSubSplitState> finishedSplitIds) {}

    @Override
    protected PubSubSplitState initializedState(PubSubSplit split) {
        //		return new PubSubSplitState(split);
        return null;
    }

    @Override
    protected PubSubSplit toSplitType(String splitId, PubSubSplitState splitState) {
        return null;
    }
}
