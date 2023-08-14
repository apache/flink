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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.blob.TestingBlobHelpers.checkFileCountForJob;
import static org.apache.flink.runtime.blob.TestingBlobHelpers.checkFilesExist;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/** Tests for {@link BlobLibraryCacheManager}. */
@RunWith(Parameterized.class)
public class BlobLibraryCacheManagerTest extends TestLogger {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Parameterized.Parameters(name = "Use system class loader: {0}")
    public static List<Boolean> useSystemClassLoader() {
        return Arrays.asList(true, false);
    }

    @Parameterized.Parameter public boolean wrapsSystemClassLoader;

    /**
     * Tests that the {@link BlobLibraryCacheManager} cleans up after the class loader leases for
     * different jobs are closed.
     */
    @Test
    public void testLibraryCacheManagerDifferentJobsCleanup() throws Exception {

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
            config.setLong(BlobServerOptions.CLEANUP_INTERVAL, 1L);

            server = new BlobServer(config, temporaryFolder.newFolder(), new VoidBlobStore());
            server.start();
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
            cache =
                    new PermanentBlobCache(
                            config,
                            temporaryFolder.newFolder(),
                            new VoidBlobStore(),
                            serverAddress);

            keys1.add(server.putPermanent(jobId1, buf));
            buf[0] += 1;
            keys1.add(server.putPermanent(jobId1, buf));
            keys2.add(server.putPermanent(jobId2, buf));

            libCache = createBlobLibraryCacheManager(cache);
            cache.registerJob(jobId1);
            cache.registerJob(jobId2);

            assertEquals(0, libCache.getNumberOfManagedJobs());
            assertEquals(0, libCache.getNumberOfReferenceHolders(jobId1));
            checkFileCountForJob(2, jobId1, server);
            checkFileCountForJob(0, jobId1, cache);
            checkFileCountForJob(1, jobId2, server);
            checkFileCountForJob(0, jobId2, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLeaseJob1 =
                    libCache.registerClassLoaderLease(jobId1);
            final UserCodeClassLoader classLoader1 =
                    classLoaderLeaseJob1.getOrResolveClassLoader(keys1, Collections.emptyList());

            assertEquals(1, libCache.getNumberOfManagedJobs());
            assertEquals(1, libCache.getNumberOfReferenceHolders(jobId1));
            assertEquals(0, libCache.getNumberOfReferenceHolders(jobId2));
            assertEquals(2, checkFilesExist(jobId1, keys1, cache, false));
            checkFileCountForJob(2, jobId1, server);
            checkFileCountForJob(2, jobId1, cache);
            assertEquals(0, checkFilesExist(jobId2, keys2, cache, false));
            checkFileCountForJob(1, jobId2, server);
            checkFileCountForJob(0, jobId2, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLeaseJob2 =
                    libCache.registerClassLoaderLease(jobId2);
            final UserCodeClassLoader classLoader2 =
                    classLoaderLeaseJob2.getOrResolveClassLoader(keys2, Collections.emptyList());
            assertThat(classLoader1, not(sameInstance(classLoader2)));

            try {
                classLoaderLeaseJob2.getOrResolveClassLoader(keys1, Collections.<URL>emptyList());
                fail("Should fail with an IllegalStateException");
            } catch (IllegalStateException e) {
                // that's what we want
            }

            try {
                classLoaderLeaseJob2.getOrResolveClassLoader(
                        keys2, Collections.singletonList(new URL("file:///tmp/does-not-exist")));
                fail("Should fail with an IllegalStateException");
            } catch (IllegalStateException e) {
                // that's what we want
            }

            assertEquals(2, libCache.getNumberOfManagedJobs());
            assertEquals(1, libCache.getNumberOfReferenceHolders(jobId1));
            assertEquals(1, libCache.getNumberOfReferenceHolders(jobId2));
            assertEquals(2, checkFilesExist(jobId1, keys1, cache, false));
            checkFileCountForJob(2, jobId1, server);
            checkFileCountForJob(2, jobId1, cache);
            assertEquals(1, checkFilesExist(jobId2, keys2, cache, false));
            checkFileCountForJob(1, jobId2, server);
            checkFileCountForJob(1, jobId2, cache);

            classLoaderLeaseJob1.release();

            assertEquals(1, libCache.getNumberOfManagedJobs());
            assertEquals(0, libCache.getNumberOfReferenceHolders(jobId1));
            assertEquals(1, libCache.getNumberOfReferenceHolders(jobId2));
            assertEquals(2, checkFilesExist(jobId1, keys1, cache, false));
            checkFileCountForJob(2, jobId1, server);
            checkFileCountForJob(2, jobId1, cache);
            assertEquals(1, checkFilesExist(jobId2, keys2, cache, false));
            checkFileCountForJob(1, jobId2, server);
            checkFileCountForJob(1, jobId2, cache);

            classLoaderLeaseJob2.release();

            assertEquals(0, libCache.getNumberOfManagedJobs());
            assertEquals(0, libCache.getNumberOfReferenceHolders(jobId1));
            assertEquals(0, libCache.getNumberOfReferenceHolders(jobId2));
            assertEquals(2, checkFilesExist(jobId1, keys1, cache, false));
            checkFileCountForJob(2, jobId1, server);
            checkFileCountForJob(2, jobId1, cache);
            assertEquals(1, checkFilesExist(jobId2, keys2, cache, false));
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
    @Test
    public void testLibraryCacheManagerCleanup() throws Exception {

        JobID jobId = new JobID();
        List<PermanentBlobKey> keys = new ArrayList<>();
        BlobServer server = null;
        PermanentBlobCache cache = null;
        BlobLibraryCacheManager libCache = null;

        final byte[] buf = new byte[128];

        try {
            Configuration config = new Configuration();
            config.setLong(BlobServerOptions.CLEANUP_INTERVAL, 1L);

            server = new BlobServer(config, temporaryFolder.newFolder(), new VoidBlobStore());
            server.start();
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
            cache =
                    new PermanentBlobCache(
                            config,
                            temporaryFolder.newFolder(),
                            new VoidBlobStore(),
                            serverAddress);

            keys.add(server.putPermanent(jobId, buf));
            buf[0] += 1;
            keys.add(server.putPermanent(jobId, buf));

            libCache = createBlobLibraryCacheManager(cache);
            cache.registerJob(jobId);

            assertEquals(0, libCache.getNumberOfManagedJobs());
            assertEquals(0, libCache.getNumberOfReferenceHolders(jobId));
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(0, jobId, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLease1 =
                    libCache.registerClassLoaderLease(jobId);
            UserCodeClassLoader classLoader1 =
                    classLoaderLease1.getOrResolveClassLoader(keys, Collections.emptyList());

            assertEquals(1, libCache.getNumberOfManagedJobs());
            assertEquals(1, libCache.getNumberOfReferenceHolders(jobId));
            assertEquals(2, checkFilesExist(jobId, keys, cache, true));
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(2, jobId, cache);

            final LibraryCacheManager.ClassLoaderLease classLoaderLease2 =
                    libCache.registerClassLoaderLease(jobId);
            final UserCodeClassLoader classLoader2 =
                    classLoaderLease2.getOrResolveClassLoader(keys, Collections.emptyList());
            assertThat(classLoader1, sameInstance(classLoader2));

            try {
                classLoaderLease1.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());
                fail("Should fail with an IllegalStateException");
            } catch (IllegalStateException e) {
                // that's what we want
            }

            try {
                classLoaderLease1.getOrResolveClassLoader(
                        keys, Collections.singletonList(new URL("file:///tmp/does-not-exist")));
                fail("Should fail with an IllegalStateException");
            } catch (IllegalStateException e) {
                // that's what we want
            }

            assertEquals(1, libCache.getNumberOfManagedJobs());
            assertEquals(2, libCache.getNumberOfReferenceHolders(jobId));
            assertEquals(2, checkFilesExist(jobId, keys, cache, true));
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(2, jobId, cache);

            classLoaderLease1.release();

            assertEquals(1, libCache.getNumberOfManagedJobs());
            assertEquals(1, libCache.getNumberOfReferenceHolders(jobId));
            assertEquals(2, checkFilesExist(jobId, keys, cache, true));
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(2, jobId, cache);

            classLoaderLease2.release();

            assertEquals(0, libCache.getNumberOfManagedJobs());
            assertEquals(0, libCache.getNumberOfReferenceHolders(jobId));
            assertEquals(2, checkFilesExist(jobId, keys, cache, true));
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

    @Test
    public void testRegisterAndDownload() throws IOException {
        assumeTrue(!OperatingSystem.isWindows()); // setWritable doesn't work on Windows.

        JobID jobId = new JobID();
        BlobServer server = null;
        PermanentBlobCache cache = null;
        BlobLibraryCacheManager libCache = null;
        File cacheDir = null;
        try {
            // create the blob transfer services
            Configuration config = new Configuration();
            config.setLong(BlobServerOptions.CLEANUP_INTERVAL, 1_000_000L);

            server = new BlobServer(config, temporaryFolder.newFolder(), new VoidBlobStore());
            server.start();
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
            cache =
                    new PermanentBlobCache(
                            config,
                            temporaryFolder.newFolder(),
                            new VoidBlobStore(),
                            serverAddress);

            // upload some meaningless data to the server
            PermanentBlobKey dataKey1 =
                    server.putPermanent(jobId, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
            PermanentBlobKey dataKey2 =
                    server.putPermanent(jobId, new byte[] {11, 12, 13, 14, 15, 16, 17, 18});

            libCache = createBlobLibraryCacheManager(cache);
            assertEquals(0, libCache.getNumberOfManagedJobs());
            checkFileCountForJob(2, jobId, server);
            checkFileCountForJob(0, jobId, cache);

            // first try to access a non-existing entry
            assertEquals(0, libCache.getNumberOfReferenceHolders(new JobID()));

            // register some BLOBs as libraries
            {
                Collection<PermanentBlobKey> keys = Collections.singleton(dataKey1);

                cache.registerJob(jobId);
                final LibraryCacheManager.ClassLoaderLease classLoaderLease1 =
                        libCache.registerClassLoaderLease(jobId);
                final UserCodeClassLoader classLoader1 =
                        classLoaderLease1.getOrResolveClassLoader(keys, Collections.emptyList());
                assertEquals(1, libCache.getNumberOfManagedJobs());
                assertEquals(1, libCache.getNumberOfReferenceHolders(jobId));
                assertEquals(1, checkFilesExist(jobId, keys, cache, true));
                checkFileCountForJob(2, jobId, server);
                checkFileCountForJob(1, jobId, cache);

                final LibraryCacheManager.ClassLoaderLease classLoaderLease2 =
                        libCache.registerClassLoaderLease(jobId);
                final UserCodeClassLoader classLoader2 =
                        classLoaderLease2.getOrResolveClassLoader(keys, Collections.emptyList());
                assertThat(classLoader1, sameInstance(classLoader2));
                assertEquals(1, libCache.getNumberOfManagedJobs());
                assertEquals(2, libCache.getNumberOfReferenceHolders(jobId));
                assertEquals(1, checkFilesExist(jobId, keys, cache, true));
                checkFileCountForJob(2, jobId, server);
                checkFileCountForJob(1, jobId, cache);

                // un-register the job
                classLoaderLease1.release();
                // still one task
                assertEquals(1, libCache.getNumberOfManagedJobs());
                assertEquals(1, libCache.getNumberOfReferenceHolders(jobId));
                assertEquals(1, checkFilesExist(jobId, keys, cache, true));
                checkFileCountForJob(2, jobId, server);
                checkFileCountForJob(1, jobId, cache);

                // unregister the task registration
                classLoaderLease2.release();
                assertEquals(0, libCache.getNumberOfManagedJobs());
                assertEquals(0, libCache.getNumberOfReferenceHolders(jobId));
                // changing the libCache registration does not influence the BLOB stores...
                checkFileCountForJob(2, jobId, server);
                checkFileCountForJob(1, jobId, cache);

                cache.releaseJob(jobId);

                // library is still cached (but not associated with job any more)
                checkFileCountForJob(2, jobId, server);
                checkFileCountForJob(1, jobId, cache);
            }

            // see BlobUtils for the directory layout
            cacheDir = cache.getStorageLocation(jobId, new PermanentBlobKey()).getParentFile();
            assertTrue(cacheDir.exists());

            // make sure no further blobs can be downloaded by removing the write
            // permissions from the directory
            assertTrue(
                    "Could not remove write permissions from cache directory",
                    cacheDir.setWritable(false, false));

            // since we cannot download this library any more, this call should fail
            try {
                cache.registerJob(jobId);
                final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                        libCache.registerClassLoaderLease(jobId);
                classLoaderLease.getOrResolveClassLoader(
                        Collections.singleton(dataKey2), Collections.emptyList());
                fail("This should fail with an IOException");
            } catch (IOException e) {
                // splendid!
                cache.releaseJob(jobId);
            }
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

    @Test(expected = IOException.class)
    public void getOrResolveClassLoader_missingBlobKey_shouldFail() throws IOException {
        final PermanentBlobKey missingKey = new PermanentBlobKey();

        final BlobLibraryCacheManager libraryCacheManager = createSimpleBlobLibraryCacheManager();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID());

        classLoaderLease.getOrResolveClassLoader(
                Collections.singletonList(missingKey), Collections.emptyList());
    }

    @Test(expected = IllegalStateException.class)
    public void getOrResolveClassLoader_closedLease_shouldFail() throws IOException {
        final BlobLibraryCacheManager libraryCacheManager = createSimpleBlobLibraryCacheManager();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID());

        classLoaderLease.release();

        classLoaderLease.getOrResolveClassLoader(Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void closingAllLeases_willReleaseUserCodeClassLoader() throws IOException {
        final TestingClassLoader classLoader = new TestingClassLoader();
        final BlobLibraryCacheManager libraryCacheManager =
                new TestingBlobLibraryCacheManagerBuilder()
                        .setClassLoaderFactory(ignored -> classLoader)
                        .build();

        final JobID jobId = new JobID();
        final LibraryCacheManager.ClassLoaderLease classLoaderLease1 =
                libraryCacheManager.registerClassLoaderLease(jobId);
        final LibraryCacheManager.ClassLoaderLease classLoaderLease2 =
                libraryCacheManager.registerClassLoaderLease(jobId);

        UserCodeClassLoader userCodeClassLoader =
                classLoaderLease1.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());

        classLoaderLease1.release();

        assertFalse(classLoader.isClosed());

        classLoaderLease2.release();

        if (wrapsSystemClassLoader) {
            assertEquals(userCodeClassLoader.asClassLoader(), ClassLoader.getSystemClassLoader());
            assertFalse(classLoader.isClosed());
        } else {
            assertTrue(classLoader.isClosed());
        }
    }

    @Test
    public void differentLeasesForSameJob_returnSameClassLoader() throws IOException {
        final BlobLibraryCacheManager libraryCacheManager = createSimpleBlobLibraryCacheManager();

        final JobID jobId = new JobID();
        final LibraryCacheManager.ClassLoaderLease classLoaderLease1 =
                libraryCacheManager.registerClassLoaderLease(jobId);
        final LibraryCacheManager.ClassLoaderLease classLoaderLease2 =
                libraryCacheManager.registerClassLoaderLease(jobId);

        final UserCodeClassLoader classLoader1 =
                classLoaderLease1.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());
        final UserCodeClassLoader classLoader2 =
                classLoaderLease2.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());

        assertThat(classLoader1, sameInstance(classLoader2));
    }

    @Test(expected = IllegalStateException.class)
    public void closingLibraryCacheManager_invalidatesAllOpenLeases() throws IOException {
        final BlobLibraryCacheManager libraryCacheManager = createSimpleBlobLibraryCacheManager();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID());

        libraryCacheManager.shutdown();

        classLoaderLease.getOrResolveClassLoader(Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void closingLibraryCacheManager_closesClassLoader() throws IOException {
        final TestingClassLoader classLoader = new TestingClassLoader();
        final BlobLibraryCacheManager libraryCacheManager =
                new TestingBlobLibraryCacheManagerBuilder()
                        .setClassLoaderFactory(ignored -> classLoader)
                        .build();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID());
        UserCodeClassLoader userCodeClassLoader =
                classLoaderLease.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());

        libraryCacheManager.shutdown();

        if (wrapsSystemClassLoader) {
            assertEquals(userCodeClassLoader.asClassLoader(), ClassLoader.getSystemClassLoader());
            assertFalse(classLoader.isClosed());
        } else {
            assertTrue(classLoader.isClosed());
        }
    }

    @Test
    public void releaseUserCodeClassLoader_willRunReleaseHooks()
            throws IOException, InterruptedException {
        final BlobLibraryCacheManager libraryCacheManager =
                new TestingBlobLibraryCacheManagerBuilder().build();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID());
        final UserCodeClassLoader userCodeClassLoader =
                classLoaderLease.getOrResolveClassLoader(
                        Collections.emptyList(), Collections.emptyList());

        final OneShotLatch releaseHookLatch = new OneShotLatch();
        userCodeClassLoader.registerReleaseHookIfAbsent("test", releaseHookLatch::trigger);

        // this should trigger the release of the class loader
        classLoaderLease.release();

        releaseHookLatch.await();
    }

    @Test
    public void releaseUserCodeClassLoader_willRegisterOnce()
            throws IOException, InterruptedException {
        final BlobLibraryCacheManager libraryCacheManager =
                new TestingBlobLibraryCacheManagerBuilder().build();

        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                libraryCacheManager.registerClassLoaderLease(new JobID());
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
                            temporaryFolder.newFolder(),
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
