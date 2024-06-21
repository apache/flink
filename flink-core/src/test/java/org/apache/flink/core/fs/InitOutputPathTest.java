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
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A test validating that the initialization of local output paths is properly synchronized. */
class InitOutputPathTest {

    @TempDir private static java.nio.file.Path tempFolder;

    /**
     * This test validates that this test case makes sense - that the error can be produced in the
     * absence of synchronization, if the threads make progress in a certain way, here enforced by
     * latches.
     */
    @Test
    void testErrorOccursUnSynchronized() throws Exception {
        // deactivate the lock to produce the original un-synchronized state
        Field lock = FileSystem.class.getDeclaredField("OUTPUT_DIRECTORY_INIT_LOCK");
        lock.setAccessible(true);

        Field modifiers = Field.class.getDeclaredField("modifiers");
        modifiers.setAccessible(true);
        modifiers.setInt(lock, lock.getModifiers() & ~Modifier.FINAL);

        lock.set(null, new NoOpLock());
        // in the original un-synchronized state, we can force the race to occur by using
        // the proper latch order to control the process of the concurrent threads
        assertThatThrownBy(() -> runTest(true)).isInstanceOf(FileNotFoundException.class);
        lock.set(null, new ReentrantLock(true));
    }

    @Test
    void testProperSynchronized() throws Exception {
        // in the synchronized variant, we cannot use the "await latches" because not
        // both threads can make process interleaved (due to the synchronization)
        // the test uses sleeps (rather than latches) to produce the same interleaving.
        // while that is not guaranteed to produce the pathological interleaving,
        // it helps to provoke it very often. together with validating that this order
        // is in fact pathological (see testErrorOccursUnSynchronized()), this gives
        // a rather confident guard
        runTest(false);
    }

    private void runTest(final boolean useAwaits) throws Exception {
        final File tempFile = TempDirUtils.newFile(tempFolder);
        final Path path1 = new Path(tempFile.getAbsolutePath(), "1");
        final Path path2 = new Path(tempFile.getAbsolutePath(), "2");

        final OneShotLatch deleteAwaitLatch1 = new OneShotLatch();
        final OneShotLatch deleteAwaitLatch2 = new OneShotLatch();
        final OneShotLatch mkdirsAwaitLatch1 = new OneShotLatch();
        final OneShotLatch mkdirsAwaitLatch2 = new OneShotLatch();

        final OneShotLatch deleteTriggerLatch1 = new OneShotLatch();
        final OneShotLatch deletetriggerLatch2 = new OneShotLatch();
        final OneShotLatch mkdirsTriggerLatch1 = new OneShotLatch();
        final OneShotLatch mkdirsTriggerLatch2 = new OneShotLatch();

        final OneShotLatch createAwaitLatch = new OneShotLatch();
        final OneShotLatch createTriggerLatch = new OneShotLatch();

        final LocalFileSystem fs1 =
                new SyncedFileSystem(
                        deleteAwaitLatch1,
                        mkdirsAwaitLatch1,
                        deleteTriggerLatch1,
                        mkdirsTriggerLatch1,
                        createAwaitLatch,
                        createTriggerLatch);

        final LocalFileSystem fs2 =
                new SyncedFileSystem(
                        deleteAwaitLatch2,
                        mkdirsAwaitLatch2,
                        deletetriggerLatch2,
                        mkdirsTriggerLatch2,
                        createAwaitLatch,
                        createTriggerLatch);

        // start the concurrent file creators
        FileCreator thread1 = new FileCreator(fs1, path1);
        FileCreator thread2 = new FileCreator(fs2, path2);
        thread1.start();
        thread2.start();

        // wait until they both decide to delete the directory
        if (useAwaits) {
            deleteAwaitLatch1.await();
            deleteAwaitLatch2.await();
        } else {
            Thread.sleep(5);
        }

        // now send off #1 to delete the directory (it will pass the 'mkdirs' fast) and wait to
        // create the file
        mkdirsTriggerLatch1.trigger();
        deleteTriggerLatch1.trigger();

        if (useAwaits) {
            createAwaitLatch.await();
        } else {
            // this needs a bit more sleep time, because here mockito is working
            Thread.sleep(100);
        }

        // now send off #2 to delete the directory - it waits at 'mkdirs'
        deletetriggerLatch2.trigger();
        if (useAwaits) {
            mkdirsAwaitLatch2.await();
        } else {
            Thread.sleep(5);
        }

        // let #1 try to create the file and see if it succeeded
        createTriggerLatch.trigger();
        if (useAwaits) {
            thread1.sync();
        } else {
            Thread.sleep(5);
        }

        // now let #1 finish up
        mkdirsTriggerLatch2.trigger();

        thread1.sync();
        thread2.sync();
    }

    // ------------------------------------------------------------------------

    private static class FileCreator extends CheckedThread {

        private final FileSystem fs;
        private final Path path;

        FileCreator(FileSystem fs, Path path) {
            this.fs = fs;
            this.path = path;
        }

        @Override
        public void go() throws Exception {
            fs.initOutPathLocalFS(path.getParent(), WriteMode.OVERWRITE, true);
            try (FSDataOutputStream out = fs.create(path, WriteMode.OVERWRITE)) {
                out.write(11);
            }
        }
    }

    // ------------------------------------------------------------------------

    private static class SyncedFileSystem extends LocalFileSystem {

        private final OneShotLatch deleteTriggerLatch;
        private final OneShotLatch mkdirsTriggerLatch;

        private final OneShotLatch deleteAwaitLatch;
        private final OneShotLatch mkdirsAwaitLatch;

        private final OneShotLatch createAwaitLatch;
        private final OneShotLatch createTriggerLatch;

        SyncedFileSystem(
                OneShotLatch deleteTriggerLatch,
                OneShotLatch mkdirsTriggerLatch,
                OneShotLatch deleteAwaitLatch,
                OneShotLatch mkdirsAwaitLatch,
                OneShotLatch createAwaitLatch,
                OneShotLatch createTriggerLatch) {

            this.deleteTriggerLatch = deleteTriggerLatch;
            this.mkdirsTriggerLatch = mkdirsTriggerLatch;
            this.deleteAwaitLatch = deleteAwaitLatch;
            this.mkdirsAwaitLatch = mkdirsAwaitLatch;
            this.createAwaitLatch = createAwaitLatch;
            this.createTriggerLatch = createTriggerLatch;
        }

        @Override
        @SneakyThrows
        public FSDataOutputStream create(final Path filePath, final WriteMode overwrite)
                throws IOException {
            checkNotNull(filePath, "filePath");

            if (exists(filePath) && overwrite == WriteMode.NO_OVERWRITE) {
                throw new FileAlreadyExistsException("File already exists: " + filePath);
            }

            final Path parent = filePath.getParent();
            if (parent != null && !mkdirs(parent)) {
                throw new IOException("Mkdirs failed to create " + parent);
            }

            final File file = pathToFile(filePath);
            createAwaitLatch.trigger();
            createTriggerLatch.await();
            return new LocalDataOutputStream(file);
        }

        @Override
        public boolean delete(Path f, boolean recursive) throws IOException {
            deleteTriggerLatch.trigger();
            try {
                deleteAwaitLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted");
            }

            return super.delete(f, recursive);
        }

        @Override
        public boolean mkdirs(Path f) throws IOException {
            mkdirsTriggerLatch.trigger();
            try {
                mkdirsAwaitLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted");
            }

            return super.mkdirs(f);
        }
    }

    // ------------------------------------------------------------------------

    @SuppressWarnings("serial")
    private static final class NoOpLock extends ReentrantLock {

        @Override
        public void lock() {}

        @Override
        public void lockInterruptibly() {}

        @Override
        public void unlock() {}
    }
}
