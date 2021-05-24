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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The BLOB cache provides access to BLOB services for permanent and transient BLOBs. */
public class BlobCacheService implements BlobService {

    /** Caching store for permanent BLOBs. */
    private final PermanentBlobCache permanentBlobCache;

    /** Store for transient BLOB files. */
    private final TransientBlobCache transientBlobCache;

    /**
     * Instantiates a new BLOB cache.
     *
     * @param blobClientConfig global configuration
     * @param blobView (distributed) blob store file system to retrieve files from first
     * @param serverAddress address of the {@link BlobServer} to use for fetching files from or
     *     {@code null} if none yet
     * @throws IOException thrown if the (local or distributed) file storage cannot be created or is
     *     not usable
     */
    public BlobCacheService(
            final Configuration blobClientConfig,
            final BlobView blobView,
            @Nullable final InetSocketAddress serverAddress)
            throws IOException {

        this(
                new PermanentBlobCache(blobClientConfig, blobView, serverAddress),
                new TransientBlobCache(blobClientConfig, serverAddress));
    }

    /**
     * Instantiates a new BLOB cache.
     *
     * @param permanentBlobCache BLOB cache to use for permanent BLOBs
     * @param transientBlobCache BLOB cache to use for transient BLOBs
     */
    public BlobCacheService(
            PermanentBlobCache permanentBlobCache, TransientBlobCache transientBlobCache) {
        this.permanentBlobCache = checkNotNull(permanentBlobCache);
        this.transientBlobCache = checkNotNull(transientBlobCache);
    }

    @Override
    public PermanentBlobCache getPermanentBlobService() {
        return permanentBlobCache;
    }

    @Override
    public TransientBlobCache getTransientBlobService() {
        return transientBlobCache;
    }

    /**
     * Sets the address of the {@link BlobServer}.
     *
     * @param blobServerAddress address of the {@link BlobServer}.
     */
    public void setBlobServerAddress(InetSocketAddress blobServerAddress) {
        permanentBlobCache.setBlobServerAddress(blobServerAddress);
        transientBlobCache.setBlobServerAddress(blobServerAddress);
    }

    @Override
    public void close() throws IOException {
        permanentBlobCache.close();
        transientBlobCache.close();
    }

    @Override
    public int getPort() {
        // NOTE: both blob stores connect to the same server!
        return permanentBlobCache.getPort();
    }
}
