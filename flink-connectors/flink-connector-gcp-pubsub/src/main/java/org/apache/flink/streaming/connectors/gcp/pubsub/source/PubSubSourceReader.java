package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** */
public class PubSubSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                Tuple2<T, Long>, T, PubSubSplit, PubSubSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubSourceReader.class);

    public PubSubSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<T, Long>>> elementsQueue,
            Supplier<PubSubSplitReader<T>> splitReaderSupplier,
            RecordEmitter<Tuple2<T, Long>, T, PubSubSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        //		TODO: right super constructor?
        super(
                elementsQueue,
                new PubSubSourceFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, PubSubSplitState> finishedSplitIds) {
        //        try {
        //            close();
        //        } catch (Exception e) {
        //            e.printStackTrace();
        //        }
    }

    @Override
    public List<PubSubSplit> snapshotState(long checkpointId) {
        // maybe match number of currently dealt-out splits (even though they are all the same)?
        return Arrays.asList(new PubSubSplit());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.info("Acknowledging Pub/Sub messages for checkpoint {}", checkpointId);
        ((PubSubSourceFetcherManager<T>) splitFetcherManager).acknowledgeMessages();
    }

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
