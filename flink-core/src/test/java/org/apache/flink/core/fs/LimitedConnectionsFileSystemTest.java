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

package org.apache.flink.core.fs;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link LimitedConnectionsFileSystem}. */
class LimitedConnectionsFileSystemTest {

    @TempDir public File tempFolder;

    @Test
    void testConstructionNumericOverflow() {
        final LimitedConnectionsFileSystem limitedFs =
                new LimitedConnectionsFileSystem(
                        LocalFileSystem.getSharedInstance(),
                        Integer.MAX_VALUE, // unlimited total
                        Integer.MAX_VALUE, // limited outgoing
                        Integer.MAX_VALUE, // unlimited incoming
                        Long.MAX_VALUE - 1, // long timeout, close to overflow
                        Long.MAX_VALUE - 1); // long timeout, close to overflow

        assertThat(limitedFs.getMaxNumOpenStreamsTotal()).isEqualTo(Integer.MAX_VALUE);
        assertThat(limitedFs.getMaxNumOpenOutputStreams()).isEqualTo(Integer.MAX_VALUE);
        assertThat(limitedFs.getMaxNumOpenInputStreams()).isEqualTo(Integer.MAX_VALUE);

        assertThat(limitedFs.getStreamOpenTimeout()).isPositive();
        assertThat(limitedFs.getStreamInactivityTimeout()).isPositive();
    }

    @Test
    void testLimitingOutputStreams() throws Exception {
        final int maxConcurrentOpen = 2;
        final int numThreads = 61;

        final LimitedConnectionsFileSystem limitedFs =
                new LimitedConnectionsFileSystem(
                        LocalFileSystem.getSharedInstance(),
                        Integer.MAX_VALUE, // unlimited total
                        maxConcurrentOpen, // limited outgoing
                        Integer.MAX_VALUE, // unlimited incoming
                        0,
                        0);

        final WriterThread[] threads = new WriterThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            Path path = new Path(File.createTempFile("junit", null, tempFolder).toURI());
            threads[i] = new WriterThread(limitedFs, path, maxConcurrentOpen, Integer.MAX_VALUE);
        }

        for (WriterThread t : threads) {
            t.start();
        }

        for (WriterThread t : threads) {
            t.sync();
        }
    }

    @Test
    void testLimitingInputStreams() throws Exception {
        final int maxConcurrentOpen = 2;
        final int numThreads = 61;

        final LimitedConnectionsFileSystem limitedFs =
                new LimitedConnectionsFileSystem(
                        LocalFileSystem.getSharedInstance(),
                        Integer.MAX_VALUE, // unlimited total
                        Integer.MAX_VALUE, // unlimited outgoing
                        maxConcurrentOpen, // limited incoming
                        0,
                        0);

        final Random rnd = new Random();

        final ReaderThread[] threads = new ReaderThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            File file = File.createTempFile("junit", null, tempFolder);
            createRandomContents(file, rnd);
            Path path = new Path(file.toURI());
            threads[i] = new ReaderThread(limitedFs, path, maxConcurrentOpen, Integer.MAX_VALUE);
        }

        for (ReaderThread t : threads) {
            t.start();
        }

        for (ReaderThread t : threads) {
            t.sync();
        }
    }

    @Test
    void testLimitingMixedStreams() throws Exception {
        final int maxConcurrentOpen = 2;
        final int numThreads = 61;

        final LimitedConnectionsFileSystem limitedFs =
                new LimitedConnectionsFileSystem(
                        LocalFileSystem.getSharedInstance(), maxConcurrentOpen); // limited total

        final Random rnd = new Random();

        final CheckedThread[] threads = new CheckedThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            File file = File.createTempFile("junit", null, tempFolder);
            Path path = new Path(file.toURI());

            if (rnd.nextBoolean()) {
                // reader thread
                createRandomContents(file, rnd);
                threads[i] =
                        new ReaderThread(limitedFs, path, Integer.MAX_VALUE, maxConcurrentOpen);
            } else {
                threads[i] =
                        new WriterThread(limitedFs, path, Integer.MAX_VALUE, maxConcurrentOpen);
            }
        }

        for (CheckedThread t : threads) {
            t.start();
        }

        for (CheckedThread t : threads) {
            t.sync();
        }
    }

    @Test
    void testOpenTimeoutOutputStreams() throws Exception {
        final long openTimeout = 50L;
        final int maxConcurrentOpen = 2;

        final LimitedConnectionsFileSystem limitedFs =
                new LimitedConnectionsFileSystem(
                        LocalFileSystem.getSharedInstance(),
                        maxConcurrentOpen, // limited total
                        openTimeout, // small opening timeout
                        0L); // infinite inactivity timeout

        // create the threads that block all streams
        final BlockingWriterThread[] threads = new BlockingWriterThread[maxConcurrentOpen];
        for (int i = 0; i < maxConcurrentOpen; i++) {
            Path path = new Path(File.createTempFile("junit", null, tempFolder).toURI());
            threads[i] =
                    new BlockingWriterThread(limitedFs, path, Integer.MAX_VALUE, maxConcurrentOpen);
            threads[i].start();
        }

        // wait until all are open
        while (limitedFs.getTotalNumberOfOpenStreams() < maxConcurrentOpen) {
            Thread.sleep(1);
        }

        // try to open another thread
        assertThatThrownBy(
                        () ->
                                limitedFs.create(
                                        new Path(
                                                File.createTempFile("junit", null, tempFolder)
                                                        .toURI()),
                                        WriteMode.OVERWRITE))
                .isInstanceOf(IOException.class);

        // clean shutdown
        for (BlockingWriterThread t : threads) {
            t.wakeup();
            t.sync();
        }
    }

    @Test
    void testOpenTimeoutInputStreams() throws Exception {
        final long openTimeout = 50L;
        final int maxConcurrentOpen = 2;

        final LimitedConnectionsFileSystem limitedFs =
                new LimitedConnectionsFileSystem(
                        LocalFileSystem.getSharedInstance(),
                        maxConcurrentOpen, // limited total
                        openTimeout, // small opening timeout
                        0L); // infinite inactivity timeout

        // create the threads that block all streams
        final Random rnd = new Random();
        final BlockingReaderThread[] threads = new BlockingReaderThread[maxConcurrentOpen];
        for (int i = 0; i < maxConcurrentOpen; i++) {
            File file = File.createTempFile("junit", null, tempFolder);
            createRandomContents(file, rnd);
            Path path = new Path(file.toURI());
            threads[i] =
                    new BlockingReaderThread(limitedFs, path, maxConcurrentOpen, Integer.MAX_VALUE);
            threads[i].start();
        }

        // wait until all are open
        while (limitedFs.getTotalNumberOfOpenStreams() < maxConcurrentOpen) {
            Thread.sleep(1);
        }

        // try to open another thread
        File file = File.createTempFile("junit", null, tempFolder);
        createRandomContents(file, rnd);
        assertThatThrownBy(() -> limitedFs.open(new Path(file.toURI())))
                .isInstanceOf(IOException.class);

        // clean shutdown
        for (BlockingReaderThread t : threads) {
            t.wakeup();
            t.sync();
        }
    }

    @Test
    void testTerminateStalledOutputStreams() throws Exception {
        final int maxConcurrentOpen = 2;
        final int numThreads = 20;

        // this testing file system has a 50 ms stream inactivity timeout
        final LimitedConnectionsFileSystem limitedFs =
                new LimitedConnectionsFileSystem(
                        LocalFileSystem.getSharedInstance(),
                        Integer.MAX_VALUE, // no limit on total streams
                        maxConcurrentOpen, // limit on output streams
                        Integer.MAX_VALUE, // no limit on input streams
                        0,
                        50); // timeout of 50 ms

        final WriterThread[] threads = new WriterThread[numThreads];
        final BlockingWriterThread[] blockers = new BlockingWriterThread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            Path path1 = new Path(File.createTempFile("junit", null, tempFolder).toURI());
            Path path2 = new Path(File.createTempFile("junit", null, tempFolder).toURI());

            threads[i] = new WriterThread(limitedFs, path1, maxConcurrentOpen, Integer.MAX_VALUE);
            blockers[i] =
                    new BlockingWriterThread(
                            limitedFs, path2, maxConcurrentOpen, Integer.MAX_VALUE);
        }

        // start normal and blocker threads
        for (int i = 0; i < numThreads; i++) {
            blockers[i].start();
            threads[i].start();
        }

        // all normal threads need to be able to finish because
        // the blockers eventually time out
        for (WriterThread t : threads) {
            try {
                t.sync();
            } catch (LimitedConnectionsFileSystem.StreamTimeoutException e) {
                // also the regular threads may occasionally get a timeout on
                // slower test machines because we set very aggressive timeouts
                // to reduce the test time
            }
        }

        // unblock all the blocking threads
        for (BlockingThread t : blockers) {
            t.wakeup();
        }
        for (BlockingThread t : blockers) {
            try {
                t.sync();
            } catch (LimitedConnectionsFileSystem.StreamTimeoutException ignored) {
            }
        }
    }

    @Test
    void testTerminateStalledInputStreams() throws Exception {
        final int maxConcurrentOpen = 2;
        final int numThreads = 20;

        // this testing file system has a 50 ms stream inactivity timeout
        final LimitedConnectionsFileSystem limitedFs =
                new LimitedConnectionsFileSystem(
                        LocalFileSystem.getSharedInstance(),
                        Integer.MAX_VALUE, // no limit on total streams
                        Integer.MAX_VALUE, // limit on output streams
                        maxConcurrentOpen, // no limit on input streams
                        0,
                        50); // timeout of 50 ms

        final Random rnd = new Random();

        final ReaderThread[] threads = new ReaderThread[numThreads];
        final BlockingReaderThread[] blockers = new BlockingReaderThread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            File file1 = File.createTempFile("junit", null, tempFolder);
            File file2 = File.createTempFile("junit", null, tempFolder);

            createRandomContents(file1, rnd);
            createRandomContents(file2, rnd);

            Path path1 = new Path(file1.toURI());
            Path path2 = new Path(file2.toURI());

            threads[i] = new ReaderThread(limitedFs, path1, maxConcurrentOpen, Integer.MAX_VALUE);
            blockers[i] =
                    new BlockingReaderThread(
                            limitedFs, path2, maxConcurrentOpen, Integer.MAX_VALUE);
        }

        // start normal and blocker threads
        for (int i = 0; i < numThreads; i++) {
            blockers[i].start();
            threads[i].start();
        }

        // all normal threads need to be able to finish because
        // the blockers eventually time out
        for (ReaderThread t : threads) {
            try {
                t.sync();
            } catch (LimitedConnectionsFileSystem.StreamTimeoutException e) {
                // also the regular threads may occasionally get a timeout on
                // slower test machines because we set very aggressive timeouts
                // to reduce the test time
            }
        }

        // unblock all the blocking threads
        for (BlockingThread t : blockers) {
            t.wakeup();
        }
        for (BlockingThread t : blockers) {
            try {
                t.sync();
            } catch (LimitedConnectionsFileSystem.StreamTimeoutException ignored) {
            }
        }
    }

    @Test
    void testTerminateStalledMixedStreams() throws Exception {
        final int maxConcurrentOpen = 2;
        final int numThreads = 20;

        final LimitedConnectionsFileSystem limitedFs =
                new LimitedConnectionsFileSystem(
                        LocalFileSystem.getSharedInstance(),
                        maxConcurrentOpen, // limited total
                        0L, // no opening timeout
                        50L); // inactivity timeout of 50 ms

        final Random rnd = new Random();

        final CheckedThread[] threads = new CheckedThread[numThreads];
        final BlockingThread[] blockers = new BlockingThread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            File file1 = File.createTempFile("junit", null, tempFolder);
            File file2 = File.createTempFile("junit", null, tempFolder);
            Path path1 = new Path(file1.toURI());
            Path path2 = new Path(file2.toURI());

            if (rnd.nextBoolean()) {
                createRandomContents(file1, rnd);
                createRandomContents(file2, rnd);
                threads[i] =
                        new ReaderThread(limitedFs, path1, maxConcurrentOpen, Integer.MAX_VALUE);
                blockers[i] =
                        new BlockingReaderThread(
                                limitedFs, path2, maxConcurrentOpen, Integer.MAX_VALUE);
            } else {
                threads[i] =
                        new WriterThread(limitedFs, path1, maxConcurrentOpen, Integer.MAX_VALUE);
                blockers[i] =
                        new BlockingWriterThread(
                                limitedFs, path2, maxConcurrentOpen, Integer.MAX_VALUE);
            }
        }

        // start normal and blocker threads
        for (int i = 0; i < numThreads; i++) {
            blockers[i].start();
            threads[i].start();
        }

        // all normal threads need to be able to finish because
        // the blockers eventually time out
        for (CheckedThread t : threads) {
            try {
                t.sync();
            } catch (LimitedConnectionsFileSystem.StreamTimeoutException e) {
                // also the regular threads may occasionally get a timeout on
                // slower test machines because we set very aggressive timeouts
                // to reduce the test time
            }
        }

        // unblock all the blocking threads
        for (BlockingThread t : blockers) {
            t.wakeup();
        }
        for (BlockingThread t : blockers) {
            try {
                t.sync();
            } catch (LimitedConnectionsFileSystem.StreamTimeoutException ignored) {
            }
        }
    }

    @Test
    void testFailingStreamsUnregister() throws Exception {
        final LimitedConnectionsFileSystem fs = new LimitedConnectionsFileSystem(new FailFs(), 1);

        assertThat(fs.getNumberOfOpenInputStreams()).isZero();
        assertThat(fs.getNumberOfOpenOutputStreams()).isZero();
        assertThat(fs.getTotalNumberOfOpenStreams()).isZero();

        assertThatThrownBy(
                        () ->
                                fs.open(
                                        new Path(
                                                File.createTempFile("junit", null, tempFolder)
                                                        .toURI())))
                .isInstanceOf(IOException.class);

        assertThatThrownBy(
                        () ->
                                fs.create(
                                        new Path(
                                                File.createTempFile("junit", null, tempFolder)
                                                        .toURI()),
                                        WriteMode.NO_OVERWRITE))
                .isInstanceOf(IOException.class);

        assertThat(fs.getNumberOfOpenInputStreams()).isZero();
        assertThat(fs.getNumberOfOpenOutputStreams()).isZero();
        assertThat(fs.getTotalNumberOfOpenStreams()).isZero();
    }

    /**
     * Tests that a slowly written output stream is not accidentally closed too aggressively, due to
     * a wrong initialization of the timestamps or bytes written that mark when the last progress
     * was checked.
     */
    @Test
    void testSlowOutputStreamNotClosed() throws Exception {
        final LimitedConnectionsFileSystem fs =
                new LimitedConnectionsFileSystem(LocalFileSystem.getSharedInstance(), 1, 0L, 1000L);

        // some competing threads
        final Random rnd = new Random();
        final ReaderThread[] threads = new ReaderThread[10];
        for (int i = 0; i < threads.length; i++) {
            File file = File.createTempFile("junit", null, tempFolder);
            createRandomContents(file, rnd);
            Path path = new Path(file.toURI());
            threads[i] = new ReaderThread(fs, path, 1, Integer.MAX_VALUE);
        }

        // open the stream we test
        try (FSDataOutputStream out =
                fs.create(
                        new Path(File.createTempFile("junit", null, tempFolder).toURI()),
                        WriteMode.OVERWRITE)) {

            // start the other threads that will try to shoot this stream down
            for (ReaderThread t : threads) {
                t.start();
            }

            // read the stream slowly.
            Thread.sleep(5);
            for (int bytesLeft = 50; bytesLeft > 0; bytesLeft--) {
                out.write(bytesLeft);
                Thread.sleep(5);
            }
        }

        // wait for clean shutdown
        for (ReaderThread t : threads) {
            t.sync();
        }
    }

    /**
     * Tests that a slowly read stream is not accidentally closed too aggressively, due to a wrong
     * initialization of the timestamps or bytes written that mark when the last progress was
     * checked.
     */
    @Test
    void testSlowInputStreamNotClosed() throws Exception {
        final File file = File.createTempFile("junit", null, tempFolder);
        createRandomContents(file, new Random(), 50);

        final LimitedConnectionsFileSystem fs =
                new LimitedConnectionsFileSystem(LocalFileSystem.getSharedInstance(), 1, 0L, 1000L);

        // some competing threads
        final WriterThread[] threads = new WriterThread[10];
        for (int i = 0; i < threads.length; i++) {
            Path path = new Path(File.createTempFile("junit", null, tempFolder).toURI());
            threads[i] = new WriterThread(fs, path, 1, Integer.MAX_VALUE);
        }

        // open the stream we test
        try (FSDataInputStream in = fs.open(new Path(file.toURI()))) {

            // start the other threads that will try to shoot this stream down
            for (WriterThread t : threads) {
                t.start();
            }

            // read the stream slowly.
            Thread.sleep(5);
            while (in.read() != -1) {
                Thread.sleep(5);
            }
        }

        // wait for clean shutdown
        for (WriterThread t : threads) {
            t.sync();
        }
    }

    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    private void createRandomContents(File file, Random rnd) throws IOException {
        createRandomContents(file, rnd, rnd.nextInt(10000) + 1);
    }

    private void createRandomContents(File file, Random rnd, int size) throws IOException {
        final byte[] data = new byte[size];
        rnd.nextBytes(data);

        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(data);
        }
    }

    // ------------------------------------------------------------------------
    //  Testing threads
    // ------------------------------------------------------------------------

    private static final class WriterThread extends CheckedThread {

        private final LimitedConnectionsFileSystem fs;

        private final Path path;

        private final int maxConcurrentOutputStreams;

        private final int maxConcurrentStreamsTotal;

        WriterThread(
                LimitedConnectionsFileSystem fs,
                Path path,
                int maxConcurrentOutputStreams,
                int maxConcurrentStreamsTotal) {

            this.fs = fs;
            this.path = path;
            this.maxConcurrentOutputStreams = maxConcurrentOutputStreams;
            this.maxConcurrentStreamsTotal = maxConcurrentStreamsTotal;
        }

        @Override
        public void go() throws Exception {

            try (FSDataOutputStream stream = fs.create(path, WriteMode.OVERWRITE)) {
                assertThat(fs.getNumberOfOpenOutputStreams())
                        .isLessThanOrEqualTo(maxConcurrentOutputStreams);
                assertThat(fs.getTotalNumberOfOpenStreams())
                        .isLessThanOrEqualTo(maxConcurrentStreamsTotal);

                final Random rnd = new Random();
                final byte[] data = new byte[rnd.nextInt(10000) + 1];
                rnd.nextBytes(data);
                stream.write(data);
            }
        }
    }

    private static final class ReaderThread extends CheckedThread {

        private final LimitedConnectionsFileSystem fs;

        private final Path path;

        private final int maxConcurrentInputStreams;

        private final int maxConcurrentStreamsTotal;

        ReaderThread(
                LimitedConnectionsFileSystem fs,
                Path path,
                int maxConcurrentInputStreams,
                int maxConcurrentStreamsTotal) {

            this.fs = fs;
            this.path = path;
            this.maxConcurrentInputStreams = maxConcurrentInputStreams;
            this.maxConcurrentStreamsTotal = maxConcurrentStreamsTotal;
        }

        @Override
        public void go() throws Exception {

            try (FSDataInputStream stream = fs.open(path)) {
                assertThat(fs.getNumberOfOpenInputStreams())
                        .isLessThanOrEqualTo(maxConcurrentInputStreams);
                assertThat(fs.getTotalNumberOfOpenStreams())
                        .isLessThanOrEqualTo(maxConcurrentStreamsTotal);

                final byte[] readBuffer = new byte[4096];

                //noinspection StatementWithEmptyBody
                while (stream.read(readBuffer) != -1) {}
            }
        }
    }

    private abstract static class BlockingThread extends CheckedThread {

        private final OneShotLatch waiter = new OneShotLatch();

        public void waitTillWokenUp() throws InterruptedException {
            waiter.await();
        }

        public void wakeup() {
            waiter.trigger();
        }
    }

    private static final class BlockingWriterThread extends BlockingThread {

        private final LimitedConnectionsFileSystem fs;

        private final Path path;

        private final int maxConcurrentOutputStreams;

        private final int maxConcurrentStreamsTotal;

        BlockingWriterThread(
                LimitedConnectionsFileSystem fs,
                Path path,
                int maxConcurrentOutputStreams,
                int maxConcurrentStreamsTotal) {

            this.fs = fs;
            this.path = path;
            this.maxConcurrentOutputStreams = maxConcurrentOutputStreams;
            this.maxConcurrentStreamsTotal = maxConcurrentStreamsTotal;
        }

        @Override
        public void go() throws Exception {

            try (FSDataOutputStream stream = fs.create(path, WriteMode.OVERWRITE)) {
                assertThat(fs.getNumberOfOpenOutputStreams())
                        .isLessThanOrEqualTo(maxConcurrentOutputStreams);
                assertThat(fs.getTotalNumberOfOpenStreams())
                        .isLessThanOrEqualTo(maxConcurrentStreamsTotal);

                final Random rnd = new Random();
                final byte[] data = new byte[rnd.nextInt(10000) + 1];
                rnd.nextBytes(data);
                stream.write(data);

                waitTillWokenUp();

                // try to write one more thing, which might/should fail with an I/O exception
                stream.write(rnd.nextInt());
            }
        }
    }

    private static final class BlockingReaderThread extends BlockingThread {

        private final LimitedConnectionsFileSystem fs;

        private final Path path;

        private final int maxConcurrentInputStreams;

        private final int maxConcurrentStreamsTotal;

        BlockingReaderThread(
                LimitedConnectionsFileSystem fs,
                Path path,
                int maxConcurrentInputStreams,
                int maxConcurrentStreamsTotal) {

            this.fs = fs;
            this.path = path;
            this.maxConcurrentInputStreams = maxConcurrentInputStreams;
            this.maxConcurrentStreamsTotal = maxConcurrentStreamsTotal;
        }

        @Override
        public void go() throws Exception {

            try (FSDataInputStream stream = fs.open(path)) {
                assertThat(fs.getNumberOfOpenInputStreams())
                        .isLessThanOrEqualTo(maxConcurrentInputStreams);
                assertThat(fs.getTotalNumberOfOpenStreams())
                        .isLessThanOrEqualTo(maxConcurrentStreamsTotal);

                final byte[] readBuffer = new byte[(int) fs.getFileStatus(path).getLen() - 1];
                assertThat(stream.read(readBuffer) != -1).isTrue();

                waitTillWokenUp();

                // try to write one more thing, which might/should fail with an I/O exception
                //noinspection ResultOfMethodCallIgnored
                stream.read();
            }
        }
    }

    // ------------------------------------------------------------------------
    //  failing file system
    // ------------------------------------------------------------------------

    private static class FailFs extends LocalFileSystem {

        @Override
        public FSDataOutputStream create(Path filePath, WriteMode overwrite) throws IOException {
            throw new IOException("test exception");
        }

        @Override
        public FSDataInputStream open(Path f) throws IOException {
            throw new IOException("test exception");
        }
    }
}
