package org.apache.flink.connector.pulsar.source.utils;

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public final class PulsarSerdeUtils {

    private PulsarSerdeUtils() {
        // No public constructor.
    }

    public static void serializeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static byte[] deserializeBytes(DataInputStream in) throws IOException {
        int size = in.readInt();
        byte[] bytes = new byte[size];
        int result = in.read(bytes);
        if (result < 0) {
            throw new IOException("Couldn't deserialize the object, wrong byte buffer.");
        }

        return bytes;
    }

    public static void serializeObject(DataOutputStream out, Object obj) throws IOException {
        Preconditions.checkNotNull(obj);

        byte[] objectBytes = InstantiationUtil.serializeObject(obj);
        serializeBytes(out, objectBytes);
    }

    public static <T> T deserializeObject(DataInputStream in) throws IOException {
        byte[] objectBytes = deserializeBytes(in);
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        try {
            return InstantiationUtil.deserializeObject(objectBytes, loader);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    public static <T> void serializeSet(
            DataOutputStream out,
            Set<T> set,
            FunctionWithException<T, byte[], IOException> serializer)
            throws IOException {
        out.writeInt(set.size());
        for (T t : set) {
            byte[] bytes = serializer.apply(t);
            serializeBytes(out, bytes);
        }
    }

    public static <T> Set<T> deserializeSet(
            DataInputStream in, FunctionWithException<byte[], T, IOException> serializer)
            throws IOException {
        int size = in.readInt();
        Set<T> set = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            byte[] bytes = deserializeBytes(in);
            T t = serializer.apply(bytes);
            set.add(t);
        }

        return set;
    }
}
