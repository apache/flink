/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import org.apache.commons.io.FileUtils;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;

/** Utility class for testing methods for blobs. */
class TestingBlobUtils {
    private TestingBlobUtils() {
        throw new UnsupportedOperationException(
                String.format("Cannot instantiate %s.", TestingBlobUtils.class.getSimpleName()));
    }

    @Nonnull
    static TransientBlobKey writeTransientBlob(
            Path storageDirectory, JobID jobId, byte[] fileContent) throws IOException {
        return (TransientBlobKey)
                writeBlob(storageDirectory, jobId, fileContent, BlobKey.BlobType.TRANSIENT_BLOB);
    }

    @Nonnull
    static PermanentBlobKey writePermanentBlob(
            Path storageDirectory, JobID jobId, byte[] fileContent) throws IOException {
        return (PermanentBlobKey)
                writeBlob(storageDirectory, jobId, fileContent, BlobKey.BlobType.PERMANENT_BLOB);
    }

    @Nonnull
    static BlobKey writeBlob(
            Path storageDirectory, JobID jobId, byte[] fileContent, BlobKey.BlobType blobType)
            throws IOException {
        final BlobKey blobKey =
                BlobKey.createKey(blobType, BlobUtils.createMessageDigest().digest(fileContent));

        final File storageLocation =
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDirectory.toString(), jobId, blobKey));
        FileUtils.createParentDirectories(storageLocation);
        FileUtils.writeByteArrayToFile(storageLocation, fileContent);

        return blobKey;
    }

    @Nonnull
    static BlobServer createServer(Path tempDir) throws IOException {
        return createServer(tempDir, new Configuration(), new VoidBlobStore());
    }

    @Nonnull
    static BlobServer createServer(Path tempDir, Configuration config) throws IOException {
        return createServer(tempDir, config, new VoidBlobStore());
    }

    @Nonnull
    static BlobServer createServer(Path tempDir, Configuration config, BlobStore serverStore)
            throws IOException {
        return new BlobServer(config, getServerDir(tempDir), serverStore);
    }

    @Nonnull
    static PermanentBlobCache createPermanentCache(Path tempDir, BlobServer server)
            throws IOException {
        return createPermanentCache(tempDir, new Configuration(), getServerAddress(server));
    }

    @Nonnull
    static PermanentBlobCache createPermanentCache(
            Path tempDir, Configuration config, BlobServer server) throws IOException {
        return createPermanentCache(tempDir, config, getServerAddress(server));
    }

    @Nonnull
    static PermanentBlobCache createPermanentCache(
            Path tempDir, Configuration config, BlobServer server, BlobCacheSizeTracker tracker)
            throws IOException {
        return createPermanentCache(tempDir, config, getServerAddress(server), tracker);
    }

    @Nonnull
    static PermanentBlobCache createPermanentCache(
            Path tempDir, Configuration config, InetSocketAddress serverAddress)
            throws IOException {
        return createPermanentCache(tempDir, config, serverAddress, null);
    }

    @Nonnull
    static TransientBlobCache createTransientCache(Path tempDir, BlobServer server)
            throws IOException {
        return new TransientBlobCache(
                new Configuration(), getCacheDir(tempDir), getServerAddress(server));
    }

    @Nonnull
    static PermanentBlobCache createPermanentCache(
            Path tempDir,
            Configuration config,
            InetSocketAddress serverAddress,
            BlobCacheSizeTracker tracker)
            throws IOException {
        if (tracker == null) {
            return new PermanentBlobCache(
                    config, getCacheDir(tempDir), new VoidBlobStore(), serverAddress);
        }

        return new PermanentBlobCache(
                config, getCacheDir(tempDir), new VoidBlobStore(), serverAddress, tracker);
    }

    @Nonnull
    static Tuple2<BlobServer, BlobCacheService> createServerAndCache(Path tempDir)
            throws IOException {
        return createServerAndCache(
                tempDir, new Configuration(), new VoidBlobStore(), new VoidBlobStore());
    }

    @Nonnull
    static Tuple2<BlobServer, BlobCacheService> createServerAndCache(
            Path tempDir, Configuration config) throws IOException {
        return createServerAndCache(tempDir, config, new VoidBlobStore(), new VoidBlobStore());
    }

    @Nonnull
    static Tuple2<BlobServer, BlobCacheService> createServerAndCache(
            Path tempDir, BlobStore serverStore, BlobStore cacheStore) throws IOException {
        return createServerAndCache(tempDir, new Configuration(), serverStore, cacheStore);
    }

    @Nonnull
    static Tuple2<BlobServer, BlobCacheService> createServerAndCache(
            Path tempDir, Configuration config, BlobStore serverStore, BlobStore cacheStore)
            throws IOException {
        return createServerAndCache(tempDir, config, config, serverStore, cacheStore);
    }

    @Nonnull
    static Tuple2<BlobServer, BlobCacheService> createServerAndCache(
            Path tempDir,
            Configuration serverConfig,
            Configuration cacheConfig,
            BlobStore serverStore,
            BlobStore cacheStore)
            throws IOException {
        BlobServer server = createServer(tempDir, serverConfig, serverStore);
        BlobCacheService cache =
                new BlobCacheService(
                        cacheConfig,
                        getCacheDir(tempDir),
                        cacheStore,
                        new InetSocketAddress("localhost", server.getPort()));

        return Tuple2.of(server, cache);
    }

    @Nonnull
    static Tuple2<BlobServer, BlobCacheService> createFailingServerAndCache(
            Path tempDir, BlobStore serverStore, int numAccept, int numFailures)
            throws IOException {
        Configuration config = new Configuration();
        BlobServer server =
                new TestingFailingBlobServer(
                        config, getServerDir(tempDir), serverStore, numAccept, numFailures);
        BlobCacheService cache =
                new BlobCacheService(
                        config,
                        getCacheDir(tempDir),
                        new VoidBlobStore(),
                        new InetSocketAddress("localhost", server.getPort()));

        return Tuple2.of(server, cache);
    }

    private static InetSocketAddress getServerAddress(BlobServer server) {
        return new InetSocketAddress("localhost", server.getPort());
    }

    private static File getServerDir(Path tempDir) {
        return tempDir.resolve("server").toFile();
    }

    private static File getCacheDir(Path tempDir) {
        return tempDir.resolve("cache").toFile();
    }
}
