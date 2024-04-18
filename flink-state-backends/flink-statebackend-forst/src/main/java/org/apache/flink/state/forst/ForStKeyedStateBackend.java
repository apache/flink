/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Disposable;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;

/**
 * A KeyedStateBackend that stores its state in {@code ForSt}. This state backend can store very
 * large state that exceeds memory even disk to remote storage. TODO: Support to implement the new
 * interface of KeyedStateBackend
 */
public class ForStKeyedStateBackend<K> implements Disposable {

    private static final Logger LOG = LoggerFactory.getLogger(ForStKeyedStateBackend.class);

    /** The key serializer. */
    protected final TypeSerializer<K> keySerializer;

    /** The container of ForSt options. */
    private final ForStResourceContainer optionsContainer;

    /**
     * We are not using the default column family for Flink state ops, but we still need to remember
     * this handle so that we can close it properly when the backend is closed. Note that the one
     * returned by {@link RocksDB#open(String)} is different from that by {@link
     * RocksDB#getDefaultColumnFamily()}, probably it's a bug of RocksDB java API.
     */
    private final ColumnFamilyHandle defaultColumnFamily;

    /** The native metrics monitor. */
    private final ForStNativeMetricMonitor nativeMetricMonitor;

    /**
     * Our ForSt database. The different k/v states that we have don't each have their own ForSt
     * instance. They all write to this instance but to their own column family.
     */
    protected final RocksDB db;

    // mark whether this backend is already disposed and prevent duplicate disposing
    private boolean disposed = false;

    public ForStKeyedStateBackend(
            ForStResourceContainer optionsContainer,
            TypeSerializer<K> keySerializer,
            RocksDB db,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            ForStNativeMetricMonitor nativeMetricMonitor) {
        this.optionsContainer = Preconditions.checkNotNull(optionsContainer);
        this.keySerializer = keySerializer;
        this.db = db;
        this.defaultColumnFamily = defaultColumnFamilyHandle;
        this.nativeMetricMonitor = nativeMetricMonitor;
    }

    /** Should only be called by one thread, and only after all accesses to the DB happened. */
    @Override
    public void dispose() {
        if (this.disposed) {
            return;
        }

        // IMPORTANT: null reference to signal potential async checkpoint workers that the db was
        // disposed, as
        // working on the disposed object results in SEGFAULTS.
        if (db != null) {

            // Metric collection occurs on a background thread. When this method returns
            // it is guaranteed that thr ForSt reference has been invalidated
            // and no more metric collection will be attempted against the database.
            if (nativeMetricMonitor != null) {
                nativeMetricMonitor.close();
            }

            IOUtils.closeQuietly(defaultColumnFamily);

            // ... and finally close the DB instance ...
            IOUtils.closeQuietly(db);

            LOG.info(
                    "Closed ForSt State Backend. Cleaning up ForSt local working directory {}, remote working directory {}.",
                    optionsContainer.getLocalBasePath(),
                    optionsContainer.getRemoteBasePath());

            try {
                optionsContainer.clearDirectories();
            } catch (Exception ex) {
                LOG.warn(
                        "Could not delete ForSt local working directory {}, remote working directory {}.",
                        optionsContainer.getLocalBasePath(),
                        optionsContainer.getRemoteBasePath(),
                        ex);
            }

            IOUtils.closeQuietly(optionsContainer);
        }
        this.disposed = true;
    }

    @VisibleForTesting
    File getLocalBasePath() {
        return optionsContainer.getLocalBasePath();
    }

    @VisibleForTesting
    URI getRemoteBasePath() {
        return optionsContainer.getRemoteBasePath();
    }
}
