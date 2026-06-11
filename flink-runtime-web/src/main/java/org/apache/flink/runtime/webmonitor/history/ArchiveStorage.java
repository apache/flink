/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.webmonitor.history;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Abstraction for the storage backend used by the {@link HistoryServer} to access archived job
 * data.
 *
 * <p>Implementations can be backed by the local file system, RocksDB, or any other storage medium.
 *
 * <p>The archive JSON will be stored in different entry types, and each implementation can decide
 * whether to read the full content or not.
 *
 * @param <Entry> Type of the storage entries.
 */
public interface ArchiveStorage<Entry> extends Closeable {

    /**
     * Returns whether the entry identified by {@code key} exists in this storage.
     *
     * @param key storage key (typically the request path, e.g. {@code jobs/xxx/config.json})
     * @return {@code true} if the entry exists
     */
    boolean exists(String key) throws IOException;

    /**
     * Returns the entry identified by {@code key} from this storage.
     *
     * @param key storage key
     * @return the entry, or null if the entry does not exist
     * @throws IOException if the entry cannot be read
     */
    @Nullable
    Entry getEntry(String key) throws IOException;

    /**
     * Stores the entry identified by {@code key} in this storage.
     *
     * @param key storage key
     * @param archiveContent the archive content to store, this type is string because the archive
     *     content is always a JSON String
     * @throws IOException if the entry cannot be written
     */
    void putArchiveContent(String key, String archiveContent) throws IOException;

    /**
     * Deletes the entry identified by {@code key} from this storage.
     *
     * @param key storage key
     * @throws IOException if the entry cannot be deleted
     */
    void delete(String key) throws IOException;

    /**
     * Deletes all entries with key starting with {@code keyPrefix} from this storage.
     *
     * <p>Such as deleting all archived files for a given job or application.
     *
     * @param keyPrefix key prefix
     * @throws IOException if any entries cannot be deleted
     */
    void deleteEntriesByPrefix(String keyPrefix) throws IOException;

    /**
     * Returns the entries identified by {@code prefix} from this storage.
     *
     * @param prefix storage key prefix
     * @return the entries
     * @throws IOException if any entries cannot be read
     */
    List<Entry> getEntriesByPrefix(String prefix) throws IOException;

    /**
     * Read the archive content from the entry.
     *
     * @param entry the entry to read
     * @return the archive content
     * @throws IOException if the entry cannot be read
     */
    String readArchiveContent(Entry entry) throws IOException;
}
