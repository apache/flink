package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/** */
public class PubSubSplitSerializer implements SimpleVersionedSerializer<PubSubSplit> {
    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PubSubSplit obj) throws IOException {
        return new byte[0];
    }

    @Override
    public PubSubSplit deserialize(int version, byte[] serialized) throws IOException {
        //		TODO:
        return new PubSubSplit();
    }
}
