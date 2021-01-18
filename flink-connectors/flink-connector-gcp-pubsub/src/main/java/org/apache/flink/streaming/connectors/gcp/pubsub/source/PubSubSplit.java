package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.connector.source.SourceSplit;

/** */
public class PubSubSplit implements SourceSplit {
    @Override
    public String splitId() {
        return "0";
    }
}
