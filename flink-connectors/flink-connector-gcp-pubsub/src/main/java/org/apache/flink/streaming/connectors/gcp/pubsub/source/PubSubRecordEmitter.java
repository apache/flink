package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/** @param <T> */
public class PubSubRecordEmitter<T> implements RecordEmitter<Tuple2<T, Long>, T, PubSubSplitState> {

    @Override
    public void emitRecord(
            Tuple2<T, Long> element, SourceOutput<T> output, PubSubSplitState splitState)
            throws Exception {
        output.collect(element.f0, element.f1);
    }
}
