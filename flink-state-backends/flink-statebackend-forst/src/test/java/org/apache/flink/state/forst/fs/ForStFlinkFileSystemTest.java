/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.fs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.ByteBufferReadable;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.local.LocalDataInputStream;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.state.forst.fs.cache.BundledCacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.FileBasedCache;
import org.apache.flink.state.forst.fs.cache.FileCacheEntry;
import org.apache.flink.state.forst.fs.cache.SizeBasedCacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.SpaceBasedCacheLimitPolicy;
import org.apache.flink.state.forst.fs.filemapping.MappingEntry;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.state.forst.ForStOptions.CACHE_LRU_ACCESS_BEFORE_PROMOTION;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ForStFlinkFileSystem}. */
@ExtendWith(ParameterizedTestExtension.class)
public class ForStFlinkFileSystemTest {

    @TempDir static Path tempDir;

    @Parameters(name = "FileBasedCache: {0}")
    public static List<Object[]> modes() {
        return Arrays.asList(
                new Object[][] {
                    {null},
                    {
                        new FileBasedCache(
                                new Configuration(),
                                new SizeBasedCacheLimitPolicy(1024 * 3, 64 * 1024 * 1024),
                                FileSystem.getLocalFileSystem(),
                                new org.apache.flink.core.fs.Path(tempDir.toString() + "/cache"),
                                null)
                    }
                });
    }

    @Parameter public FileBasedCache fileBasedCache;

    @Test
    void testReadAndWriteWithByteBuffer() throws Exception {
        ForStFlinkFileSystem fileSystem =
                ForStFlinkFileSystem.get(
                        new URI(tempDir.toString()),
                        new org.apache.flink.core.fs.Path(tempDir.toString()),
                        null);
        testReadAndWriteWithByteBuffer(fileSystem);
    }

    @TestTemplate
    void testPositionedRead() throws Exception {
        ForStFlinkFileSystem fileSystem =
                new ForStFlinkFileSystem(
                        new ByteBufferReadableLocalFileSystem(),
                        tempDir.toString(),
                        tempDir.toString(),
                        fileBasedCache);
        testReadAndWriteWithByteBuffer(fileSystem);
    }

    private void testReadAndWriteWithByteBuffer(ForStFlinkFileSystem fileSystem) throws Exception {
        org.apache.flink.core.fs.Path testFilePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/temp.sst");
        final int attempt = 200;

        // Test write with ByteBuffer
        ByteBufferWritableFSDataOutputStream outputStream = fileSystem.create(testFilePath);
        ByteBuffer writeBuffer = ByteBuffer.allocate(20);
        for (int i = 0; i < attempt; i++) {
            writeBuffer.clear();
            writeBuffer.position(2);
            writeBuffer.putLong(i);
            writeBuffer.putLong(i * 2);
            writeBuffer.flip();
            writeBuffer.position(2);
            outputStream.write(writeBuffer);
        }
        outputStream.flush();
        outputStream.close();

        // Test sequential read with ByteBuffer
        ByteBufferReadableFSDataInputStream inputStream = fileSystem.open(testFilePath);
        inputStream.seek(0);
        ByteBuffer readBuffer = ByteBuffer.allocate(20);
        for (int i = 0; i < attempt; i++) {
            readBuffer.clear();
            readBuffer.position(1);
            readBuffer.limit(17);
            int read = inputStream.readFully(readBuffer);
            assertThat(read).isEqualTo(16);
            assertThat(readBuffer.getLong(1)).isEqualTo(i);
            assertThat(readBuffer.getLong(9)).isEqualTo(i * 2);
        }
        inputStream.close();

        // Test random read with ByteBuffer concurrently
        ByteBufferReadableFSDataInputStream randomInputStream = fileSystem.open(testFilePath);
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (int index = 0; index < attempt; index++) {
            futureList.add(
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    ByteBuffer randomReadBuffer = ByteBuffer.allocate(20);
                                    for (int i = 0; i < attempt; i += 2) {
                                        randomReadBuffer.clear();
                                        randomReadBuffer.position(1);
                                        randomReadBuffer.limit(17);
                                        int read =
                                                randomInputStream.readFully(
                                                        i * 16L, randomReadBuffer);
                                        assertThat(read).isEqualTo(16);
                                        assertThat(randomReadBuffer.getLong(1)).isEqualTo(i);
                                        assertThat(randomReadBuffer.getLong(9)).isEqualTo(i * 2L);
                                    }
                                } catch (Exception ex) {
                                    throw new CompletionException(ex);
                                }
                            }));
        }
        FutureUtils.waitForAll(futureList).get();
        inputStream.close();

        assertThat(fileSystem.exists(testFilePath)).isTrue();
        fileSystem.delete(testFilePath, true);
        assertThat(fileSystem.exists(testFilePath)).isFalse();
    }

    @TestTemplate
    void testReadExceedingFileSize() throws Exception {
        ForStFlinkFileSystem fileSystem =
                new ForStFlinkFileSystem(
                        new ByteBufferReadableLocalFileSystem(),
                        tempDir.toString(),
                        tempDir.toString(),
                        fileBasedCache);

        org.apache.flink.core.fs.Path testFilePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/temp-file");
        try (ByteBufferWritableFSDataOutputStream outputStream = fileSystem.create(testFilePath)) {
            outputStream.write(1);
        }

        try (ByteBufferReadableFSDataInputStream inputStream = fileSystem.open(testFilePath)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(2);
            inputStream.readFully(0, byteBuffer);
            inputStream.readFully(byteBuffer);
        }
    }

    @TestTemplate
    void testMiscFileInLocal() throws IOException {
        org.apache.flink.core.fs.Path remotePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/remote");
        org.apache.flink.core.fs.Path localPath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/local");
        ForStFlinkFileSystem fileSystem =
                new ForStFlinkFileSystem(
                        new ByteBufferReadableLocalFileSystem(),
                        remotePath.toString(),
                        localPath.toString(),
                        fileBasedCache);
        fileSystem.mkdirs(remotePath);
        fileSystem.mkdirs(localPath);

        org.apache.flink.core.fs.Path dbFilePath =
                new org.apache.flink.core.fs.Path(remotePath, "CURRENT");
        ByteBufferWritableFSDataOutputStream os = fileSystem.create(dbFilePath);
        org.apache.flink.core.fs.Path sourceFileRealPath =
                fileSystem.getMappingEntry(dbFilePath).getSourcePath();
        os.write(233);
        os.sync();
        os.close();
        assertThat(localPath.getFileSystem().exists(sourceFileRealPath)).isTrue();
        ByteBufferReadableFSDataInputStream is =
                fileSystem.open(new org.apache.flink.core.fs.Path(remotePath, "CURRENT"));
        assertThat(is.read()).isEqualTo(233);
        is.close();
    }

    @RepeatedTest(300)
    void testSstFileInCache() throws IOException {
        final Map<String, Gauge<?>> registeredGauges = new HashMap<>();
        final Map<String, Counter> registeredCounters = new HashMap<>();
        org.apache.flink.core.fs.Path remotePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/remote");
        org.apache.flink.core.fs.Path localPath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/local");
        org.apache.flink.core.fs.Path cachePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/tmp-cache");
        BundledCacheLimitPolicy cacheLimitPolicy =
                new BundledCacheLimitPolicy(
                        new SpaceBasedCacheLimitPolicy(new File(cachePath.toString()), 0, 0),
                        new SizeBasedCacheLimitPolicy(250, 250));
        UnregisteredMetricsGroup metricsGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public <C extends Counter> C counter(String name, C counter) {
                        registeredCounters.put(name, counter);
                        return counter;
                    }

                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        registeredGauges.put(name, gauge);
                        return gauge;
                    }
                };
        FileBasedCache cache =
                new FileBasedCache(
                        new Configuration(),
                        cacheLimitPolicy,
                        FileSystem.getLocalFileSystem(),
                        cachePath,
                        metricsGroup);
        ForStFlinkFileSystem fileSystem =
                new ForStFlinkFileSystem(
                        new ByteBufferReadableLocalFileSystem(),
                        remotePath.toString(),
                        localPath.toString(),
                        cache);
        fileSystem.mkdirs(remotePath);
        fileSystem.mkdirs(localPath);
        byte[] tmpBytes = new byte[233];
        org.apache.flink.core.fs.Path sstRemotePath1 =
                new org.apache.flink.core.fs.Path(remotePath, "1.sst");
        ByteBufferWritableFSDataOutputStream os1 = fileSystem.create(sstRemotePath1);
        org.apache.flink.core.fs.Path sstRealPath1 =
                fileSystem.getMappingEntry(sstRemotePath1).getSourcePath();
        assertThat(sstRealPath1).isNotNull();
        org.apache.flink.core.fs.Path cachePath1 =
                new org.apache.flink.core.fs.Path(cachePath, sstRealPath1.getName());
        os1.write(tmpBytes);
        os1.write(76);
        os1.write(89);
        os1.sync();
        os1.close();
        assertThat(fileSystem.exists(sstRemotePath1)).isTrue();
        assertThat(cachePath.getFileSystem().exists(cachePath1)).isTrue();
        assertThat(registeredGauges.get("forst.fileCache.usedBytes").getValue()).isEqualTo(235L);
        assertThat(registeredCounters.get("forst.fileCache.hit").getCount()).isEqualTo(0L);
        assertThat(registeredCounters.get("forst.fileCache.miss").getCount()).isEqualTo(0L);
        FileCacheEntry cacheEntry1 =
                cache.get(cachePath.getPath() + "/" + sstRealPath1.getName(), false);
        assertThat(cacheEntry1).isNotNull();
        assertThat(cacheEntry1.getReferenceCount()).isEqualTo(1);

        ByteBufferReadableFSDataInputStream is = fileSystem.open(sstRemotePath1);

        assertThat(is.read(tmpBytes)).isEqualTo(233);
        assertThat(cacheEntry1.getReferenceCount()).isEqualTo(1);
        assertThat(registeredCounters.get("forst.fileCache.hit").getCount()).isEqualTo(0L);
        assertThat(registeredCounters.get("forst.fileCache.miss").getCount()).isEqualTo(0L);

        // set flink thread to enable the metrics
        FileBasedCache.setFlinkThread();
        assertThat(is.read()).isEqualTo(76);
        assertThat(registeredCounters.get("forst.fileCache.hit").getCount()).isEqualTo(1L);

        // evict
        org.apache.flink.core.fs.Path sstRemotePath2 =
                new org.apache.flink.core.fs.Path(remotePath, "2.sst");
        ByteBufferWritableFSDataOutputStream os2 = fileSystem.create(sstRemotePath2);
        org.apache.flink.core.fs.Path sstRealPath2 =
                fileSystem.getMappingEntry(sstRemotePath2).getSourcePath();
        assertThat(sstRealPath2).isNotNull();
        org.apache.flink.core.fs.Path cachePath2 =
                new org.apache.flink.core.fs.Path(cachePath, sstRealPath2.getName());
        os2.write(tmpBytes);
        os2.sync();
        os2.close();
        assertThat(fileSystem.exists(sstRemotePath1)).isTrue();
        assertThat(cachePath.getFileSystem().exists(cachePath2)).isTrue();
        assertThat(cacheEntry1.getReferenceCount()).isEqualTo(0);

        // test link and deleted by reference
        long waitDeleted = 0L;
        while (waitDeleted < 30000L && cachePath.getFileSystem().exists(cachePath1)) {
            try {
                Thread.sleep(5);
                waitDeleted += 5;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        assertThat(cachePath.getFileSystem().exists(cachePath1)).isFalse();

        assertThat(registeredGauges.get("forst.fileCache.usedBytes").getValue()).isEqualTo(233L);
        // read after evict
        assertThat(is.read()).isEqualTo(89);

        // more file to fill the cold link
        org.apache.flink.core.fs.Path sstRemotePath3 =
                new org.apache.flink.core.fs.Path(remotePath, "3.sst");
        ByteBufferWritableFSDataOutputStream os3 = fileSystem.create(sstRemotePath3);
        os3.write(tmpBytes);
        os3.sync();
        os3.close();

        // more access to loadback
        for (int i = 0; i < CACHE_LRU_ACCESS_BEFORE_PROMOTION.defaultValue() - 1; i++) {
            assertThat(registeredGauges.get("forst.fileCache.usedBytes").getValue())
                    .isEqualTo(233L);
            is.seek(0);
        }
        assertThat(registeredGauges.get("forst.fileCache.usedBytes").getValue()).isEqualTo(235L);

        is.close();

        // test link and deleted by reference
        long waitLoaded = 0L;
        while (waitLoaded < 30000L && cacheEntry1.getReferenceCount() <= 0) {
            try {
                Thread.sleep(5);
                waitLoaded += 5;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        assertThat(cacheEntry1.getReferenceCount()).isEqualTo(1);
        org.apache.flink.core.fs.Path sstRemotePath4 =
                new org.apache.flink.core.fs.Path(remotePath, "4.sst");
        fileSystem.link(sstRemotePath1, sstRemotePath4);
        assertThat(cacheEntry1.getReferenceCount()).isEqualTo(1);
        assertThat(fileSystem.exists(sstRemotePath4)).isTrue();
        fileSystem.delete(sstRemotePath1, false);
        assertThat(fileSystem.exists(sstRemotePath1)).isFalse();
        assertThat(fileSystem.exists(sstRemotePath4)).isTrue();
        assertThat(cacheEntry1.getReferenceCount()).isEqualTo(1);
        assertThat(registeredGauges.get("forst.fileCache.usedBytes").getValue()).isEqualTo(235L);
        fileSystem.delete(sstRemotePath4, false);
        assertThat(cacheEntry1.getReferenceCount()).isEqualTo(0);
        assertThat(registeredGauges.get("forst.fileCache.usedBytes").getValue()).isEqualTo(0L);

        FileBasedCache.unsetFlinkThread();
        cache.close();
    }

    @Test
    public void testFileStatusAndExist() throws IOException {
        org.apache.flink.core.fs.Path remotePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/remote");
        org.apache.flink.core.fs.Path localPath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/local");
        ForStFlinkFileSystem fileSystem =
                new ForStFlinkFileSystem(
                        new ByteBufferReadableLocalFileSystem(),
                        remotePath.toString(),
                        localPath.toString(),
                        null);
        fileSystem.mkdirs(remotePath);
        fileSystem.mkdirs(localPath);
        org.apache.flink.core.fs.Path sstRemotePath1 =
                new org.apache.flink.core.fs.Path(remotePath, "1.sst");
        ByteBufferWritableFSDataOutputStream os1 = fileSystem.create(sstRemotePath1);
        assertThat(fileSystem.getFileStatus(sstRemotePath1)).isNotNull();
        assertThat(fileSystem.getFileStatus(sstRemotePath1))
                .isInstanceOf(ForStFlinkFileSystem.DummyFSFileStatus.class);
        assertThat(fileSystem.getFileStatus(sstRemotePath1).getPath()).isEqualTo(sstRemotePath1);
        assertThat(fileSystem.exists(sstRemotePath1)).isTrue();
        assertThat(fileSystem.listStatus(remotePath)).hasSize(1);
        assertFileStatusAndBlockLocations(fileSystem, fileSystem.getFileStatus(sstRemotePath1));
        os1.write(1);
        os1.close();
        assertThat(fileSystem.getFileStatus(sstRemotePath1)).isNotNull();
        assertThat(fileSystem.getFileStatus(sstRemotePath1))
                .isNotInstanceOf(ForStFlinkFileSystem.DummyFSFileStatus.class);
        assertThat(fileSystem.getFileStatus(sstRemotePath1).getPath()).isEqualTo(sstRemotePath1);
        assertThat(fileSystem.getFileStatus(sstRemotePath1).getLen()).isEqualTo(1L);
        assertThat(fileSystem.exists(sstRemotePath1)).isTrue();
        assertThat(fileSystem.listStatus(remotePath)).hasSize(1);
        assertFileStatusAndBlockLocations(fileSystem, fileSystem.getFileStatus(sstRemotePath1));
    }

    @Test
    public void testNotOwnedFileStatus() throws IOException {
        org.apache.flink.core.fs.Path remotePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/remote");
        org.apache.flink.core.fs.Path localPath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/local");
        ForStFlinkFileSystem fileSystem =
                new ForStFlinkFileSystem(
                        // Return dummy file status which differs from real local file system
                        new LocalFileSystem() {
                            @Override
                            public FileStatus getFileStatus(org.apache.flink.core.fs.Path path) {
                                return new ForStFlinkFileSystem.DummyFSFileStatus(path);
                            }
                        },
                        remotePath.toString(),
                        localPath.toString(),
                        null);
        fileSystem.mkdirs(remotePath);
        fileSystem.mkdirs(localPath);
        org.apache.flink.core.fs.Path sstRemotePath1 =
                new org.apache.flink.core.fs.Path(remotePath, "1.sst");
        ByteBufferWritableFSDataOutputStream os1 = fileSystem.create(sstRemotePath1);
        os1.write(1);
        os1.close();

        // Mock restore procedure, getFileStatus should not use local file system to access when the
        // ownership is given to Flink
        MappingEntry mappingEntry = fileSystem.getMappingEntry(sstRemotePath1);
        assertThat(mappingEntry).isNotNull();
        assertThat(mappingEntry.getSourcePath()).isNotNull();
        FileStateHandle remoteFileStateHandle =
                new FileStateHandle(mappingEntry.getSourcePath(), 1L);
        fileSystem.registerReusedRestoredFile(
                mappingEntry.getSourcePath().toString(), remoteFileStateHandle, sstRemotePath1);
        fileSystem.giveUpOwnership(sstRemotePath1, remoteFileStateHandle);
        assertThat(fileSystem.getFileStatus(mappingEntry.getSourcePath()).getLen())
                .isNotEqualTo(
                        FileSystem.getLocalFileSystem()
                                .getFileStatus(mappingEntry.getSourcePath())
                                .getLen());
    }

    @Test
    public void testOverride() throws IOException {
        org.apache.flink.core.fs.Path remotePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/remote");
        org.apache.flink.core.fs.Path localPath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/local");
        ForStFlinkFileSystem fileSystem =
                new ForStFlinkFileSystem(
                        new ByteBufferReadableLocalFileSystem(),
                        remotePath.toString(),
                        localPath.toString(),
                        null);
        fileSystem.mkdirs(remotePath);
        fileSystem.mkdirs(localPath);
        org.apache.flink.core.fs.Path sstRemotePath1 =
                new org.apache.flink.core.fs.Path(remotePath, "1.sst");
        ByteBufferWritableFSDataOutputStream os1 = fileSystem.create(sstRemotePath1);
        os1.write(76);
        os1.close();
        assertThat(fileSystem.exists(sstRemotePath1)).isTrue();
        ByteBufferReadableFSDataInputStream is = fileSystem.open(sstRemotePath1);
        assertThat(is.read()).isEqualTo(76);

        // rewrite
        ByteBufferWritableFSDataOutputStream os2 = fileSystem.create(sstRemotePath1);
        os2.write(79);
        os2.close();
        assertThat(fileSystem.exists(sstRemotePath1)).isTrue();
        is = fileSystem.open(sstRemotePath1);
        assertThat(is.read()).isEqualTo(79);
    }

    @Test
    public void testWriteStreamSharable() throws Exception {
        org.apache.flink.core.fs.Path remotePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/remote");
        org.apache.flink.core.fs.Path localPath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/local");
        org.apache.flink.core.fs.Path cachePath =
                new org.apache.flink.core.fs.Path(tempDir.toString() + "/tmp-cache");
        BundledCacheLimitPolicy cacheLimitPolicy =
                new BundledCacheLimitPolicy(
                        new SpaceBasedCacheLimitPolicy(new File(cachePath.toString()), 0, 0),
                        new SizeBasedCacheLimitPolicy(250, 250));
        FileBasedCache cache =
                new FileBasedCache(
                        new Configuration(),
                        cacheLimitPolicy,
                        FileSystem.getLocalFileSystem(),
                        cachePath,
                        new UnregisteredMetricsGroup());
        ForStFlinkFileSystem fileSystem =
                new ForStFlinkFileSystem(
                        new ByteBufferReadableLocalFileSystem(),
                        remotePath.toString(),
                        localPath.toString(),
                        cache);

        // A write stream is create by other thread and can used by another thread.
        ExecutorService executor = Executors.newFixedThreadPool(1);
        org.apache.flink.core.fs.Path sstRemotePath =
                new org.apache.flink.core.fs.Path(remotePath, "1.sst");
        AtomicReference<ByteBufferWritableFSDataOutputStream> writeStream = new AtomicReference<>();
        executor.submit(
                        () -> {
                            FileSystemSafetyNet.initializeSafetyNetForThread();
                            try {
                                writeStream.set(fileSystem.create(sstRemotePath));
                                writeStream.get().write(1);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            } finally {
                                FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
                            }
                        })
                .get(100, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(writeStream.get()).isNotNull();
        // won't throw exception here
        writeStream.get().write(2);
        writeStream.get().close();
        ByteBufferReadableFSDataInputStream is = fileSystem.open(sstRemotePath);
        assertThat(is.read()).isEqualTo(1);
        assertThat(is.read()).isEqualTo(2);
    }

    private static void assertFileStatusAndBlockLocations(
            FileSystem fileSystem, FileStatus fileStatus) throws IOException {
        BlockLocation[] blockLocations =
                fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        Arrays.sort(blockLocations, Comparator.comparingLong(BlockLocation::getOffset));
        long offset = 0L;
        for (BlockLocation blockLocation : blockLocations) {
            assertThat(blockLocation.getOffset()).isEqualTo(offset);
            offset += blockLocation.getLength();
        }
        assertThat(offset).isEqualTo(fileStatus.getLen());
    }

    private static class ByteBufferReadableLocalFileSystem extends LocalFileSystem {

        @Override
        public FSDataInputStream open(org.apache.flink.core.fs.Path path) throws IOException {
            final File file = pathToFile(path);
            return new ByteBufferReadableLocalDataInputStream(file);
        }
    }

    private static class ByteBufferReadableLocalDataInputStream extends LocalDataInputStream
            implements ByteBufferReadable {

        private final long totalFileSize;

        public ByteBufferReadableLocalDataInputStream(File file) throws IOException {
            super(file);
            this.totalFileSize = file.length();
        }

        @Override
        public synchronized int read(ByteBuffer byteBuffer) throws IOException {
            assertThat((long) byteBuffer.remaining()).isLessThanOrEqualTo(totalFileSize);
            byte[] tmp = new byte[byteBuffer.remaining()];
            read(tmp, 0, tmp.length);
            byteBuffer.put(tmp);
            return tmp.length;
        }

        @Override
        public synchronized int read(long position, ByteBuffer byteBuffer) throws IOException {
            assertThat(position + byteBuffer.remaining()).isLessThanOrEqualTo(totalFileSize);
            byte[] tmp = new byte[byteBuffer.remaining()];
            long currPosition = getPos();
            seek(position);
            read(tmp, 0, tmp.length);
            seek(currPosition);
            byteBuffer.put(tmp);
            return tmp.length;
        }
    }
}
