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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.blob.TestingBlobHelpers.checkFileCountForApplication;
import static org.apache.flink.runtime.blob.TestingBlobHelpers.checkFileCountForJob;
import static org.apache.flink.runtime.blob.TestingBlobHelpers.checkFilesExist;
import static org.apache.flink.testutils.junit.utils.TempDirUtils.newFolder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for {@link BlobLibraryCacheManager}. */
@ExtendWith(ParameterizedTestExtension.class)
class BlobLibraryCacheManagerTest {

    @TempDir private Path temporaryFolder;

    @Parameters(name = "Use system class loader: {0}")
    private static List<Boolean> useSystemClassLoader() {
        return List.of(true, false);
    }

    @Parameter private boolean wrapsSystemClassLoader;

    private final ApplicationID applicationId = new ApplicationID();

    /**
     * Tests that the {@link BlobLibraryCacheManager} cleans up after the class loader leases for
     * different jobs are closed.
     */
    @TestTemplate
    void testLibraryCacheManagerDifferentJobsCleanup() throws Exception {

        JobID jobId1 = new JobID();
        JobID jobId2 = new JobID();
        List<PermanentBlobKey> keys1 = new ArrayList<>();
        List<PermanentBlobKey> keys2 = new ArrayList<>();
        BlobServer server = null;
        PermanentBlobCache cache = null;
        BlobLibraryCacheManager libCache = null;

        final byte[] buf = new byte[128];

        try {
            Configuration config = new Configuration();
            config.set(BlobServerOptions.CLEANUP_INTERVAL, 1L);

            server = new BlobServer(config, newFolder(temporaryFolder), new VoidBlobStore());
            server.start();
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
            cache =
                    new PermanentBlobCache(
                            config, newFolder(temporaryFolder), new VoidBlobStore(), serverAddress);

            keys1.add(server.putPermanent(jobId1, buf));
            buf[0] += 1;
            keys1.add(server.putPermanent(jobId1, buf));
            keys2.add(server.putPermanent(jobId2, buf));

            libCache = createBlobLibraryCacheManager(cache);
            cache.registerJob(jobId1, applicationId);
            cache.registerJob(jobId2, applicationId);

            assertThat(libCache.getNumberOfManagedJobs()).isZero();
            assertThat(libCache.getNumberOfReferenceHolders(jobId1)).isZero();
            checkFileCountForJob(2, jobId1, server);
            checkFileCountForJob(0, jobId1, cache);
            checkFileCountForJob(1, jobId2, server);
            checkFileCountForJob(0, jobId2, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLeaseJob1 =
                    libCache.registerClassLoaderLease(jobId1, applicationId);
            final UserCodeClassLoader classLoader1 =
                    classLoaderLeaseJob1.getOrResolveClassLoader(keys1, Collections.emptyList());

            assertThat(libCache.getNumberOfManagedJobs()).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId1)).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId2)).isZero();
            assertThat(checkFilesExist(jobId1, keys1, cache, false)).isEqualTo(2);
            checkFileCountForJob(2, jobId1, server);
            checkFileCountForJob(2, jobId1, cache);
            assertThat(checkFilesExist(jobId2, keys2, cache, false)).isZero();
            checkFileCountForJob(1, jobId2, server);
            checkFileCountForJob(0, jobId2, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLeaseJob2 =
                    libCache.registerClassLoaderLease(jobId2, applicationId);
            final UserCodeClassLoader classLoader2 =
                    classLoaderLeaseJob2.getOrResolveClassLoader(keys2, Collections.emptyList());
            assertThat(classLoader1).isNotSameAs(classLoader2);

            assertThatThrownBy(
                            () ->
                                    classLoaderLeaseJob2.getOrResolveClassLoader(
                                            keys1, Collections.<URL>emptyList()))
                    .isInstanceOf(IllegalStateException.class);

            assertThatThrownBy(
                            () ->
                                    classLoaderLeaseJob2.getOrResolveClassLoader(
                                            keys2,
                                            Collections.singletonList(
                                                    new URL("file:///tmp/does-not-exist"))))
                    .isInstanceOf(IllegalStateException.class);

            assertThat(libCache.getNumberOfManagedJobs()).isEqualTo(2);
            assertThat(libCache.getNumberOfReferenceHolders(jobId1)).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId2)).isOne();
            assertThat(checkFilesExist(jobId1, keys1, cache, false)).isEqualTo(2);
            checkFileCountForJob(2, jobId1, server);
            checkFileCountForJob(2, jobId1, cache);
            assertThat(checkFilesExist(jobId2, keys2, cache, false)).isOne();
            checkFileCountForJob(1, jobId2, server);
            checkFileCountForJob(1, jobId2, cache);

            classLoaderLeaseJob1.release();

            assertThat(libCache.getNumberOfManagedJobs()).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId1)).isZero();
            assertThat(libCache.getNumberOfReferenceHolders(jobId2)).isOne();
            assertThat(checkFilesExist(jobId1, keys1, cache, false)).isEqualTo(2);
            checkFileCountForJob(2, jobId1, server);
            checkFileCountForJob(2, jobId1, cache);
            assertThat(checkFilesExist(jobId2, keys2, cache, false)).isOne();
            checkFileCountForJob(1, jobId2, server);
            checkFileCountForJob(1, jobId2, cache);

            classLoaderLeaseJob2.release();

            assertThat(libCache.getNumberOfManagedJobs()).isZero();
            assertThat(libCache.getNumberOfReferenceHolders(jobId1)).isZero();
            assertThat(libCache.getNumberOfReferenceHolders(jobId2)).isZero();
            assertThat(checkFilesExist(jobId1, keys1, cache, false)).isEqualTo(2);
            checkFileCountForJob(2, jobId1, server);
            checkFileCountForJob(2, jobId1, cache);
            assertThat(checkFilesExist(jobId2, keys2, cache, false)).isOne();
            checkFileCountForJob(1, jobId2, server);
            checkFileCountForJob(1, jobId2, cache);

            // only PermanentBlobCache#releaseJob() calls clean up files (tested in
            // BlobCacheCleanupTest etc.
        } finally {
            if (libCache != null) {
                libCache.shutdown();
            }

            // should have been closed by the libraryCacheManager, but just in case
            if (cache != null) {
                cache.close();
            }

            if (server != null) {
                server.close();
            }
        }
    }

    /**
     * Tests that the {@link BlobLibraryCacheManager} cleans up after all class loader leases for a
     * single job a closed.
     */
    @TestTemplate
    void testLibraryCacheManagerCleanup() throws Exception {

        JobID jobId = new JobID();
        List<PermanentBlobKey> keys = new ArrayList<>();
        BlobServer server = null;
        PermanentBlobCache cache = null;
        BlobLibraryCacheManager libCache = null;

        final byte[] buf = new byte[128];

        try {
            Configuration config = new Configuration();
            config.set(BlobServerOptions.CLEANUP_INTERVAL, 1L);

            server = new BlobServer(config, newFolder(temporaryFolder), new VoidBlobStore());
            server.start();
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
            cache =
                    new PermanentBlobCache(
                            config, newFolder(temporaryFolder), new VoidBlobStore(), serverAddress);

            keys.add(server.putPermanent(jobId, buf));
            buf[0] += 1;
            keys.add(server.putPermanent(jobId, buf));

            libCache = createBlobLibraryCacheManager(cache);
            cache.registerJob(jobId, applicationId);

            assertThat(libCache.getNumberOfManagedJobs()).isZero();
            assertThat(libCache.getNumberOfReferenceHolders(jobId)).isZero();
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(0, jobId, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLease1 =
                    libCache.registerClassLoaderLease(jobId, applicationId);
            UserCodeClassLoader classLoader1 =
                    classLoaderLease1.getOrResolveClassLoader(keys, Collections.emptyList());

            assertThat(libCache.getNumberOfManagedJobs()).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId)).isOne();
            assertThat(checkFilesExist(jobId, keys, cache, true)).isEqualTo(2);
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(2, jobId, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLease2 =
                    libCache.registerClassLoaderLease(jobId, applicationId);
            final UserCodeClassLoader classLoader2 =
                    classLoaderLease2.getOrResolveClassLoader(keys, Collections.emptyList());
            assertThat(classLoader1).isSameAs(classLoader2);

            assertThatThrownBy(
                            () ->
                                    classLoaderLease1.getOrResolveClassLoader(
                                            Collections.emptyList(), Collections.emptyList()))
                    .isInstanceOf(IllegalStateException.class);

            assertThatThrownBy(
                            () ->
                                    classLoaderLease1.getOrResolveClassLoader(
                                            keys,
                                            Collections.singletonList(
                                                    new URL("file:///tmp/does-not-exist"))))
                    .isInstanceOf(IllegalStateException.class);

            assertThat(libCache.getNumberOfManagedJobs()).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId)).isEqualTo(2);
            assertThat(checkFilesExist(jobId, keys, cache, true)).isEqualTo(2);
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(2, jobId, cache);

            classLoaderLease1.release();

            assertThat(libCache.getNumberOfManagedJobs()).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId)).isOne();
            assertThat(checkFilesExist(jobId, keys, cache, true)).isEqualTo(2);
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(2, jobId, cache);

            classLoaderLease2.release();

            assertThat(libCache.getNumberOfManagedJobs()).isZero();
            assertThat(libCache.getNumberOfReferenceHolders(jobId)).isZero();
            assertThat(checkFilesExist(jobId, keys, cache, true)).isEqualTo(2);
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(2, jobId, cache);

            // only PermanentBlobCache#releaseJob() calls clean up files (tested in
            // BlobCacheCleanupTest etc.
        } finally {
            if (libCache != null) {
                libCache.shutdown();
            }

            // should have been closed by the libraryCacheManager, but just in case
            if (cache != null) {
                cache.close();
            }

            if (server != null) {
                server.close();
            }
        }
    }

    @TestTemplate
    void testRegisterAndDownload() throws IOException {
        assumeThat(OperatingSystem.isWindows()).isFalse(); // setWritable doesn't work on Windows.

        JobID jobId = new JobID();
        BlobServer server = null;
        PermanentBlobCache cache = null;
        BlobLibraryCacheManager libCache = null;
        File cacheDir = null;
        try {
            // create the blob transfer services
            Configuration config = new Configuration();
            config.set(BlobServerOptions.CLEANUP_INTERVAL, 1_000_000L);

            server = new BlobServer(config, newFolder(temporaryFolder), new VoidBlobStore());
            server.start();
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
            cache =
                    new PermanentBlobCache(
                            config, newFolder(temporaryFolder), new VoidBlobStore(), serverAddress);

            // upload some meaningless data to the server
            PermanentBlobKey dataKey1 =
                    server.putPermanent(jobId, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
            PermanentBlobKey dataKey2 =
                    server.putPermanent(jobId, new byte[] {11, 12, 13, 14, 15, 16, 17, 18});

            libCache = createBlobLibraryCacheManager(cache);
            assertThat(libCache.getNumberOfManagedJobs()).isZero();
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(0, jobId, cache);

            // first try to access a non-existing entry
            assertThat(libCache.getNumberOfReferenceHolders(new JobID())).isZero();

            // register some BLOBs as libraries
            {
                Collection<PermanentBlobKey> keys = Collections.singleton(dataKey1);

                cache.registerJob(jobId, applicationId);
                final LibraryCacheManager.ClassLoaderLease classLoaderLease1 =
                        libCache.registerClassLoaderLease(jobId, applicationId);
                final UserCodeClassLoader classLoader1 =
                        classLoaderLease1.getOrResolveClassLoader(keys, Collections.emptyList());
                assertThat(libCache.getNumberOfManagedJobs()).isOne();
                assertThat(libCache.getNumberOfReferenceHolders(jobId)).isOne();
                assertThat(checkFilesExist(jobId, keys, cache, true)).isOne();
                checkFileCountForJob(2, jobId, server);
                checkFileCountForJob(1, jobId, cache);

                final LibraryCacheManager.ClassLoaderLease classLoaderLease2 =
                        libCache.registerClassLoaderLease(jobId, applicationId);
                final UserCodeClassLoader classLoader2 =
                        classLoaderLease2.getOrResolveClassLoader(keys, Collections.emptyList());
                assertThat(classLoader1).isSameAs(classLoader2);
                assertThat(libCache.getNumberOfManagedJobs()).isOne();
                assertThat(libCache.getNumberOfReferenceHolders(jobId)).isEqualTo(2);
                assertThat(checkFilesExist(jobId, keys, cache, true)).isOne();
                checkFileCountForJob(2, jobId, server);
                checkFileCountForJob(1, jobId, cache);

                // un-register the job
                classLoaderLease1.release();
                // still one task
                assertThat(libCache.getNumberOfManagedJobs()).isOne();
                assertThat(libCache.getNumberOfReferenceHolders(jobId)).isOne();
                assertThat(checkFilesExist(jobId, keys, cache, true)).isOne();
                checkFileCountForJob(2, jobId, server);
                checkFileCountForJob(1, jobId, cache);

                // unregister the task registration
                classLoaderLease2.release();
                assertThat(libCache.getNumberOfManagedJobs()).isZero();
                assertThat(libCache.getNumberOfReferenceHolders(jobId)).isZero();
                // changing the libCache registration does not influence the BLOB stores...
                checkFileCountForJob(2, jobId, server);
                checkFileCountForJob(1, jobId, cache);

                cache.releaseJob(jobId, applicationId);

                // library is still cached (but not associated with job any more)
                checkFileCountForJob(2, jobId, server);
                checkFileCountForJob(1, jobId, cache);
            }

            // see BlobUtils for the directory layout
            cacheDir = cache.getStorageLocation(jobId, new PermanentBlobKey()).getParentFile();
            assertThat(cacheDir).exists();

            // make sure no further blobs can be downloaded by removing the write
            // permissions from the directory
            assertThat(cacheDir.setWritable(false, false))
                    .as("Could not remove write permissions from cache directory")
                    .isTrue();

            // since we cannot download this library any more, this call should fail
            cache.registerJob(jobId, applicationId);
            final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                    libCache.registerClassLoaderLease(jobId, applicationId);
            assertThatThrownBy(
                            () ->
                                    classLoaderLease.getOrResolveClassLoader(
                                            Collections.singleton(dataKey2),
                                            Collections.emptyList()))
                    .isInstanceOf(IOException.class);
            cache.releaseJob(jobId, applicationId);
        } finally {
            if (cacheDir != null) {
                if (!cacheDir.setWritable(true, false)) {
                    System.err.println("Could not re-add write permissions to cache directory.");
                }
            }
            if (cache != null) {
                cache.close();
            }
            if (libCache != null) {
                libCache.shutdown();
            }
            if (server != null) {
                server.close();
            }
        }
    }

    @TestTemplate
    void getOrResolveClassLoader_missingBlobKey_shouldFail() throws IOException {
        final PermanentBlobKey missingKey = new PermanentBlobKey();

        final BlobLibraryCacheManager libraryCacheManager = createSimpleBlobLibraryCacheManager();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID(), applicationId);

        assertThatThrownBy(
                        () ->
                                classLoaderLease.getOrResolveClassLoader(
                                        Collections.singletonList(missingKey),
                                        Collections.emptyList()))
                .isInstanceOf(IOException.class);
    }

    @TestTemplate
    void getOrResolveClassLoader_closedLease_shouldFail() throws IOException {
        final BlobLibraryCacheManager libraryCacheManager = createSimpleBlobLibraryCacheManager();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID(), applicationId);

        classLoaderLease.release();

        assertThatThrownBy(
                        () ->
                                classLoaderLease.getOrResolveClassLoader(
                                        Collections.emptyList(), Collections.emptyList()))
                .isInstanceOf(IllegalStateException.class);
    }

    @TestTemplate
    void closingAllLeases_willReleaseUserCodeClassLoader() throws IOException {
        final TestingClassLoader classLoader = new TestingClassLoader();
        final BlobLibraryCacheManager libraryCacheManager =
                new TestingBlobLibraryCacheManagerBuilder()
                        .setClassLoaderFactory(ignored -> classLoader)
                        .build();

        final JobID jobId = new JobID();
        final LibraryCacheManager.ClassLoaderLease classLoaderLease1 =
                libraryCacheManager.registerClassLoaderLease(jobId, applicationId);
        final LibraryCacheManager.ClassLoaderLease classLoaderLease2 =
                libraryCacheManager.registerClassLoaderLease(jobId, applicationId);

        UserCodeClassLoader userCodeClassLoader =
                classLoaderLease1.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());

        classLoaderLease1.release();

        assertThat(classLoader.isClosed()).isFalse();

        classLoaderLease2.release();

        if (wrapsSystemClassLoader) {
            assertThat(userCodeClassLoader.asClassLoader())
                    .isEqualTo(ClassLoader.getSystemClassLoader());
            assertThat(classLoader.isClosed()).isFalse();
        } else {
            assertThat(classLoader.isClosed()).isTrue();
        }
    }

    @TestTemplate
    void differentLeasesForSameJob_returnSameClassLoader() throws IOException {
        final BlobLibraryCacheManager libraryCacheManager = createSimpleBlobLibraryCacheManager();

        final JobID jobId = new JobID();
        final LibraryCacheManager.ClassLoaderLease classLoaderLease1 =
                libraryCacheManager.registerClassLoaderLease(jobId, applicationId);
        final LibraryCacheManager.ClassLoaderLease classLoaderLease2 =
                libraryCacheManager.registerClassLoaderLease(jobId, applicationId);

        final UserCodeClassLoader classLoader1 =
                classLoaderLease1.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());
        final UserCodeClassLoader classLoader2 =
                classLoaderLease2.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());

        assertThat(classLoader1).isSameAs(classLoader2);
    }

    @TestTemplate
    void closingLibraryCacheManager_invalidatesAllOpenLeases() throws IOException {
        final BlobLibraryCacheManager libraryCacheManager = createSimpleBlobLibraryCacheManager();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID(), applicationId);

        libraryCacheManager.shutdown();

        assertThatThrownBy(
                        () ->
                                classLoaderLease.getOrResolveClassLoader(
                                        Collections.emptyList(), Collections.emptyList()))
                .isInstanceOf(IllegalStateException.class);
    }

    @TestTemplate
    void closingLibraryCacheManager_closesClassLoader() throws IOException {
        final TestingClassLoader classLoader = new TestingClassLoader();
        final BlobLibraryCacheManager libraryCacheManager =
                new TestingBlobLibraryCacheManagerBuilder()
                        .setClassLoaderFactory(ignored -> classLoader)
                        .build();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID(), applicationId);
        UserCodeClassLoader userCodeClassLoader =
                classLoaderLease.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());

        libraryCacheManager.shutdown();

        if (wrapsSystemClassLoader) {
            assertThat(userCodeClassLoader.asClassLoader())
                    .isEqualTo(ClassLoader.getSystemClassLoader());
            assertThat(classLoader.isClosed()).isFalse();
        } else {
            assertThat(classLoader.isClosed()).isTrue();
        }
    }

    @TestTemplate
    void releaseUserCodeClassLoader_willRunReleaseHooks() throws IOException, InterruptedException {
        final BlobLibraryCacheManager libraryCacheManager =
                new TestingBlobLibraryCacheManagerBuilder().build();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID(), applicationId);
        final UserCodeClassLoader userCodeClassLoader =
                classLoaderLease.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());

        final OneShotLatch releaseHookLatch = new OneShotLatch();
        userCodeClassLoader.registerReleaseHookIfAbsent("test", releaseHookLatch::trigger);

        // this should trigger the release of the class loader
        classLoaderLease.release();

        releaseHookLatch.await();
    }

    @TestTemplate
    void releaseUserCodeClassLoader_willRegisterOnce() throws IOException, InterruptedException {
        final BlobLibraryCacheManager libraryCacheManager =
                new TestingBlobLibraryCacheManagerBuilder().build();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID(), applicationId);
        final UserCodeClassLoader userCodeClassLoader =
                classLoaderLease.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());

        final OneShotLatch releaseHookLatch = new OneShotLatch();
        userCodeClassLoader.registerReleaseHookIfAbsent("test", releaseHookLatch::trigger);
        userCodeClassLoader.registerReleaseHookIfAbsent(
                "test",
                () -> {
                    throw new RuntimeException("This hook is not expected to be executed");
                });

        classLoaderLease.release();

        // this will wait forever if the second hook gets registered
        releaseHookLatch.await();
    }

    /**
     * A {@link PermanentBlobKey} carries a random component on top of the content hash, so
     * re-uploading an unchanged JAR - as happens when a JobManager fails over and resubmits the job
     * - yields a key that is unequal to the one the cached class loader was resolved with. Such a
     * key must resolve to the cached class loader, while a key referring to different content must
     * still be rejected (FLINK-32212).
     */
    @TestTemplate
    void libraryBlobsAreComparedByContentNotByBlobKey() throws Exception {
        final JobID jobId = new JobID();
        final byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        final byte[] otherContent = new byte[] {8, 7, 6, 5, 4, 3, 2, 1};

        BlobServer server = null;
        PermanentBlobCache cache = null;
        BlobLibraryCacheManager libCache = null;

        try {
            Configuration config = new Configuration();
            config.set(BlobServerOptions.CLEANUP_INTERVAL, 1_000_000L);

            server = new BlobServer(config, newFolder(temporaryFolder), new VoidBlobStore());
            server.start();
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
            cache =
                    new PermanentBlobCache(
                            config, newFolder(temporaryFolder), new VoidBlobStore(), serverAddress);

            final PermanentBlobKey key = server.putPermanent(jobId, content);
            final PermanentBlobKey reuploadedKey = server.putPermanent(jobId, content);
            final PermanentBlobKey otherKey = server.putPermanent(jobId, otherContent);

            assertThat(reuploadedKey).isNotEqualTo(key);
            assertThat(reuploadedKey.getHash()).isEqualTo(key.getHash());

            libCache = createBlobLibraryCacheManager(cache);
            cache.registerJob(jobId, applicationId);

            final LibraryCacheManager.ClassLoaderLease lease =
                    libCache.registerClassLoaderLease(jobId, applicationId);
            final UserCodeClassLoader classLoader =
                    lease.getOrResolveClassLoader(
                            Collections.singletonList(key), Collections.emptyList());

            assertThat(
                            lease.getOrResolveClassLoader(
                                    Collections.singletonList(reuploadedKey),
                                    Collections.emptyList()))
                    .isSameAs(classLoader);

            // a duplicate does not add content, so the required set is still the same
            assertThat(lease.getOrResolveClassLoader(List.of(key, key), Collections.emptyList()))
                    .isSameAs(classLoader);

            assertThatThrownBy(
                            () ->
                                    lease.getOrResolveClassLoader(
                                            Collections.singletonList(otherKey),
                                            Collections.emptyList()))
                    .isInstanceOf(IllegalStateException.class);

            lease.release();
        } finally {
            if (libCache != null) {
                libCache.shutdown();
            }
            if (cache != null) {
                cache.close();
            }
            if (server != null) {
                server.close();
            }
        }
    }

    private BlobLibraryCacheManager createSimpleBlobLibraryCacheManager() throws IOException {
        return new TestingBlobLibraryCacheManagerBuilder().build();
    }

    private BlobLibraryCacheManager createBlobLibraryCacheManager(
            PermanentBlobCache permanentBlobCache) throws IOException {
        return new TestingBlobLibraryCacheManagerBuilder()
                .setPermanentBlobCache(permanentBlobCache)
                .build();
    }

    private final class TestingBlobLibraryCacheManagerBuilder {
        private PermanentBlobService permanentBlobCache;
        private BlobLibraryCacheManager.ClassLoaderFactory classLoaderFactory =
                BlobLibraryCacheManager.defaultClassLoaderFactory(
                        FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
                        new String[0],
                        null,
                        true);

        private TestingBlobLibraryCacheManagerBuilder() throws IOException {
            final Configuration blobClientConfig = new Configuration();
            this.permanentBlobCache =
                    new PermanentBlobCache(
                            blobClientConfig,
                            newFolder(temporaryFolder),
                            new VoidBlobStore(),
                            null);
        }

        public TestingBlobLibraryCacheManagerBuilder setPermanentBlobCache(
                PermanentBlobService permanentBlobCache) {
            this.permanentBlobCache = permanentBlobCache;
            return this;
        }

        public TestingBlobLibraryCacheManagerBuilder setClassLoaderFactory(
                BlobLibraryCacheManager.ClassLoaderFactory classLoaderFactory) {
            this.classLoaderFactory = classLoaderFactory;
            return this;
        }

        BlobLibraryCacheManager build() {
            return new BlobLibraryCacheManager(
                    permanentBlobCache, classLoaderFactory, wrapsSystemClassLoader);
        }
    }

    /** Tests that jobs with the same ApplicationID share the same jar. */
    @TestTemplate
    void testApplicationLevelJarSharing() throws Exception {
        JobID jobId1 = new JobID();
        JobID jobId2 = new JobID();
        List<PermanentBlobKey> keys = new ArrayList<>();
        BlobServer server = null;
        PermanentBlobCache cache = null;
        BlobLibraryCacheManager libCache = null;

        final byte[] buf = new byte[128];

        try {
            Configuration config = new Configuration();
            config.set(BlobServerOptions.CLEANUP_INTERVAL, 1L);

            server = new BlobServer(config, newFolder(temporaryFolder), new VoidBlobStore());
            server.start();

            cache =
                    new PermanentBlobCache(
                            config,
                            newFolder(temporaryFolder),
                            new VoidBlobStore(),
                            new InetSocketAddress("localhost", server.getPort()));

            keys.add(server.putPermanent(applicationId, buf));

            libCache = createBlobLibraryCacheManager(cache);
            cache.registerJob(jobId1, applicationId);
            cache.registerJob(jobId2, applicationId);

            assertThat(libCache.getNumberOfManagedJobs()).isZero();
            assertThat(libCache.getNumberOfReferenceHolders(jobId1)).isZero();
            assertThat(libCache.getNumberOfReferenceHolders(jobId2)).isZero();
            checkFileCountForJob(0, jobId1, server);
            checkFileCountForJob(0, jobId1, cache);
            checkFileCountForJob(0, jobId2, server);
            checkFileCountForJob(0, jobId2, cache);
            checkFileCountForApplication(1, applicationId, server);
            checkFileCountForApplication(0, applicationId, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLeaseJob1 =
                    libCache.registerClassLoaderLease(jobId1, applicationId);
            final UserCodeClassLoader classLoader1 =
                    classLoaderLeaseJob1.getOrResolveClassLoader(keys, Collections.emptyList());

            assertThat(libCache.getNumberOfManagedJobs()).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId1)).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId2)).isZero();
            checkFileCountForJob(0, jobId1, server);
            checkFileCountForJob(0, jobId1, cache);
            checkFileCountForJob(0, jobId2, server);
            checkFileCountForJob(0, jobId2, cache);
            checkFileCountForApplication(1, applicationId, server);
            checkFileCountForApplication(1, applicationId, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLeaseJob2 =
                    libCache.registerClassLoaderLease(jobId2, applicationId);
            final UserCodeClassLoader classLoader2 =
                    classLoaderLeaseJob2.getOrResolveClassLoader(keys, Collections.emptyList());

            // Classloaders are not the same
            assertThat(classLoader1).isNotSameAs(classLoader2);

            classLoaderLeaseJob1.release();
            classLoaderLeaseJob2.release();
            cache.releaseJob(jobId1, applicationId);
            cache.releaseJob(jobId2, applicationId);

            verifyApplicationCleanup(cache, applicationId, keys);
        } finally {
            if (libCache != null) {
                libCache.shutdown();
            }
            if (cache != null) {
                cache.close();
            }
            if (server != null) {
                server.close();
            }
        }
    }

    /** Tests that jobs with different ApplicationIDs do not share the same classloader. */
    @TestTemplate
    void testDifferentApplicationsNotSharing() throws Exception {
        JobID jobId1 = new JobID();
        JobID jobId2 = new JobID();
        ApplicationID applicationId1 = new ApplicationID();
        ApplicationID applicationId2 = new ApplicationID();
        List<PermanentBlobKey> keys1 = new ArrayList<>();
        List<PermanentBlobKey> keys2 = new ArrayList<>();
        BlobServer server = null;
        PermanentBlobCache cache = null;
        BlobLibraryCacheManager libCache = null;

        final byte[] buf = new byte[128];

        try {
            Configuration config = new Configuration();
            config.set(BlobServerOptions.CLEANUP_INTERVAL, 1L);

            server = new BlobServer(config, newFolder(temporaryFolder), new VoidBlobStore());
            server.start();

            cache =
                    new PermanentBlobCache(
                            config,
                            newFolder(temporaryFolder),
                            new VoidBlobStore(),
                            new InetSocketAddress("localhost", server.getPort()));

            keys1.add(server.putPermanent(applicationId1, buf));
            buf[0] += 1;
            keys1.add(server.putPermanent(applicationId1, buf));
            keys2.add(server.putPermanent(applicationId2, buf));

            libCache = createBlobLibraryCacheManager(cache);
            cache.registerJob(jobId1, applicationId1);
            cache.registerJob(jobId2, applicationId2);

            checkFileCountForApplication(2, applicationId1, server);
            checkFileCountForApplication(0, applicationId1, cache);
            checkFileCountForApplication(1, applicationId2, server);
            checkFileCountForApplication(0, applicationId2, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLeaseJob1 =
                    libCache.registerClassLoaderLease(jobId1, applicationId1);
            final UserCodeClassLoader classLoader1 =
                    classLoaderLeaseJob1.getOrResolveClassLoader(keys1, Collections.emptyList());

            assertThat(libCache.getNumberOfManagedJobs()).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId1)).isOne();
            assertThat(libCache.getNumberOfReferenceHolders(jobId2)).isZero();
            checkFileCountForApplication(2, applicationId1, server);
            checkFileCountForApplication(2, applicationId1, cache);
            checkFileCountForApplication(1, applicationId2, server);
            checkFileCountForApplication(0, applicationId2, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLeaseJob2 =
                    libCache.registerClassLoaderLease(jobId2, applicationId2);
            final UserCodeClassLoader classLoader2 =
                    classLoaderLeaseJob2.getOrResolveClassLoader(keys2, Collections.emptyList());
            assertThat(classLoader1).isNotSameAs(classLoader2);

            checkFileCountForApplication(1, applicationId2, server);
            checkFileCountForApplication(1, applicationId2, cache);

            assertThatThrownBy(
                            () ->
                                    classLoaderLeaseJob2.getOrResolveClassLoader(
                                            keys1, Collections.<URL>emptyList()))
                    .isInstanceOf(IllegalStateException.class);

            classLoaderLeaseJob1.release();
            classLoaderLeaseJob2.release();
            cache.releaseJob(jobId1, applicationId1);
            cache.releaseJob(jobId2, applicationId2);

            verifyApplicationCleanup(cache, applicationId1, keys1);
            verifyApplicationCleanup(cache, applicationId2, keys2);
        } finally {
            if (libCache != null) {
                libCache.shutdown();
            }
            if (cache != null) {
                cache.close();
            }
            if (server != null) {
                server.close();
            }
        }
    }

    /**
     * Checks that BLOBs for the given <tt>applicationId</tt> are cleaned up eventually (after
     * calling {@link PermanentBlobCache#releaseJob(JobID, ApplicationID)}, which is not done by
     * this method!) (waits at most 30s).
     *
     * @param cache BLOB server
     * @param applicationId application ID
     * @param keys keys identifying BLOBs which were previously registered for the
     *     <tt>applicationId</tt>
     */
    private static void verifyApplicationCleanup(
            PermanentBlobCache cache, ApplicationID applicationId, List<? extends BlobKey> keys)
            throws InterruptedException, IOException {
        // because we cannot guarantee that there are not thread races in the build system, we
        // loop for a certain while until the references disappear
        {
            long deadline = System.currentTimeMillis() + 30_000L;
            do {
                Thread.sleep(100);
            } while (checkFilesExist(applicationId, keys, cache, false) != 0
                    && System.currentTimeMillis() < deadline);
        }

        // the blob cache should no longer contain the files
        // this fails if we exited via a timeout
        checkFileCountForApplication(0, applicationId, cache);
    }

    private static final class TestingClassLoader extends URLClassLoader {
        private boolean isClosed;

        private TestingClassLoader() {
            super(new URL[0]);
            isClosed = false;
        }

        @Override
        public void close() throws IOException {
            super.close();

            isClosed = true;
        }

        private boolean isClosed() {
            return isClosed;
        }
    }
}
