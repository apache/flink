package org.apache.flink.mongodb.streaming.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import com.google.common.collect.Lists;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple serializer for a list of {@link MongoSplit}.
 **/
public class ListMongoSplitSerializer implements SimpleVersionedSerializer<List<MongoSplit>> {

    // the version must be the same with MongoSplitSerializer
    private static final int VERSION = 1;

    public static final ListMongoSplitSerializer INSTANCE = new ListMongoSplitSerializer();

    private static final int MAGIC_NUMBER = 0xa7b255ed;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(List<MongoSplit> list) throws IOException {

        Preconditions.checkArgument(
                MongoSplitSerializer.INSTANCE.getVersion() == VERSION,
                "ListMongoSplitSerializer is not compatible with MongoSplitSerializer. " +
                        "ListMongoSplitSerializer version: " + VERSION +
                        ", MongoSplitSerializer version: "
                        + MongoSplitSerializer.INSTANCE.getVersion());

        int size = list.size();

        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        ByteBuffer intBuf = ByteBuffer.allocate(4);
        for (MongoSplit split : list) {
            byte[] splitBytes = MongoSplitSerializer.INSTANCE.serialize(split);
            intBuf.clear();
            intBuf.putInt(splitBytes.length);
            bytesOut.write(intBuf.array());
            bytesOut.write(splitBytes.length);
        }
        byte[] listBytes = bytesOut.toByteArray();

        byte[] targetBytes = new byte[8 + 4 * size + listBytes.length];

        ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(MAGIC_NUMBER);
        bb.putInt(size);
        bb.put(listBytes);

        return targetBytes;
    }

    @Override
    public List<MongoSplit> deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
                return deserializeV1(serialized);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private static List<MongoSplit> deserializeV1(byte[] serialized) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        if (bb.getInt() != MAGIC_NUMBER) {
            throw new IOException("Corrupt data: Unexpected magic number.");
        }

        final int size = bb.getInt();
        List<MongoSplit> splits = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            int splitLen = bb.getInt();
            byte[] splitBytes = new byte[splitLen];
            bb.get(splitBytes);
            splits.add(MongoSplitSerializer.INSTANCE.deserialize(VERSION, splitBytes));
        }
        return splits;
    }
}
