package org.apache.flink.runtime.webmonitor.history.kvstore;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * Interface for a key-value store used to manage FHS-specific data.
 *
 * @param <K> the type of keys maintained by this store
 * @param <V> the type of mapped values
 */
public interface KVStore<K, V> extends Closeable {

    /**
     * Puts the specified key-value pair into the store.
     *
     * @param key the key to be inserted
     * @param value the value to be associated with the key
     * @throws Exception if any error occurs during the operation
     */
    void put(K key, V value) throws Exception;

    /**
     * Gets the value associated with the specified key from the store.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with the specified key
     * @throws Exception if any error occurs during the operation or if the key is not found
     */
    V get(K key) throws Exception;

    /**
     * Deletes the key-value pair associated with the specified key from the store.
     *
     * @param key the key whose key-value pair is to be deleted
     * @throws Exception if any error occurs during the operation
     */
    void delete(K key) throws Exception;

    /**
     * Writes multiple key-value pairs into the store.
     *
     * @param entries a map of key-value pairs to be inserted
     * @throws Exception if any error occurs during the operation
     */
    void writeAll(Map<K, V> entries) throws Exception;

    /**
     * Gets all values whose keys have the specified prefix from the store.
     *
     * @param prefix the prefix of the keys to be matched
     * @return a list of values associated with keys that have the specified prefix
     * @throws Exception if any error occurs during the operation
     */
    List<V> getAllByPrefix(K prefix) throws Exception;

    /** Closes the store and releases any resources associated with it. */
    void close();
}
