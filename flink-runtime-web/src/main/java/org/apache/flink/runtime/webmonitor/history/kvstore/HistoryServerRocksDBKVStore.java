package org.apache.flink.runtime.webmonitor.history.kvstore;

import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Implementation of KVStore that uses RocksDB as the underlying data store. */
public class HistoryServerRocksDBKVStore implements KVStore<String, String> {

    private final RocksDB db;

    private static final Options dbOptions = new Options().setCreateIfMissing(true);

    public HistoryServerRocksDBKVStore(File path) throws RocksDBException {
        this.db = RocksDB.open(dbOptions, path.getAbsolutePath());
    }

    @Override
    public void put(String key, String value) throws Exception {
        Preconditions.checkArgument(!StringUtils.isEmpty(key), "Key cannot be empty or null");
        Preconditions.checkArgument(!StringUtils.isEmpty(value), "Value cannot be empty or null");
        // Key Exists and Value is Different, the existing value is replaced with the new one. There
        // is no conflict or error
        db.put(serialize(key), serialize(value));
    }

    @Override
    public String get(String key) throws Exception {
        Preconditions.checkNotNull(key, "Key cannot be null.");
        byte[] valueBytes = db.get(serialize(key));
        return valueBytes != null ? deserialize(valueBytes) : null;
    }

    @Override
    public void delete(String key) throws Exception {
        Preconditions.checkNotNull(key, "Key cannot be null.");
        db.delete(serialize(key));
    }

    @Override
    public void writeAll(Map<String, String> entries) throws Exception {
        Preconditions.checkArgument(
                !CollectionUtils.isEmpty(entries.entrySet()), "Entries cannot be empty or null");
        try (WriteBatch batch = new WriteBatch();
                WriteOptions writeOptions = new WriteOptions()) {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                Preconditions.checkNotNull(entry.getKey(), "Key in entries cannot be null.");
                Preconditions.checkNotNull(entry.getValue(), "Value in entries cannot be null.");
                batch.put(serialize(entry.getKey()), serialize(entry.getValue()));
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new Exception("Error writing batch to RocksDB", e);
        }
    }

    @Override
    public List<String> getAllByPrefix(String prefix) throws Exception {
        Preconditions.checkNotNull(prefix, "Prefix cannot be null.");
        List<String> values = new ArrayList<>();
        byte[] prefixBytes = serialize(prefix);

        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next()) {
                String key = deserialize(iterator.key());
                if (key.startsWith(prefix)) {
                    values.add(deserialize(iterator.value()));
                }
            }
        }
        return values;
    }

    @Override
    public void close() {
        db.close();
    }

    private byte[] serialize(String str) {
        return str.getBytes(UTF_8);
    }

    private String deserialize(byte[] bytes) {
        return new String(bytes, UTF_8);
    }
}
