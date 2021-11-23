package org.apache.flink.mongodb.streaming.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.bson.BsonDocument;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Simple serializer for {@link MongoSplit}.
 **/
public class MongoSplitSerializer implements SimpleVersionedSerializer<MongoSplit> {

    private static final int VERSION = 1;

    public static final MongoSplitSerializer INSTANCE = new MongoSplitSerializer();

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final int MAGIC_NUMBER = 0xa7b2325e;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(MongoSplit obj) throws IOException {
        final byte[] splitIdBytes = obj.splitId().getBytes(CHARSET);
        final byte[] queryBytes = obj.getQuery().toJson().getBytes(CHARSET);

        final byte[] targetBytes = new byte[20 + queryBytes.length + splitIdBytes.length];

        ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(MAGIC_NUMBER);
        bb.putInt(splitIdBytes.length);
        bb.put(splitIdBytes);
        bb.putInt(queryBytes.length);
        bb.put(queryBytes);
        bb.putLong(obj.getStartOffset());

        return targetBytes;
    }

    @Override
    public MongoSplit deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
                return deserializeV1(serialized);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private static MongoSplit deserializeV1(byte[] serialized) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        if (bb.getInt() != MAGIC_NUMBER) {
            throw new IOException("Corrupt data: Unexpected magic number.");
        }

        int splitIdLen = bb.getInt();
        final byte[] splitIdBytes = new byte[splitIdLen];
        bb.get(splitIdBytes);
        String splitId = new String(splitIdBytes, CHARSET);

        int queryLen = bb.getInt();
        final byte[] queryBytes = new byte[queryLen];
        bb.get(queryBytes);
        BsonDocument query = BsonDocument.parse(new String(queryBytes, CHARSET));

        long startOffset = bb.getLong();

        return new MongoSplit(splitId, query, startOffset);
    }
}
