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
import sun.misc.Unsafe;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.FileAlreadyExistsException;
import java.security.AccessController;
import java.security.PrivilegedAction;
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
        // deactivate the lockField to produce the original un-synchronized state
        Field lockField = FileSystem.class.getDeclaredField("OUTPUT_DIRECTORY_INIT_LOCK");

        setStaticFieldUsingUnsafe(lockField, new NoOpLock());
        // in the original un-synchronized state, we can force the race to occur by using
        // the proper latch order to control the process of the concurrent threads
        assertThatThrownBy(() -> runTest(true)).isInstanceOf(FileNotFoundException.class);
        setStaticFieldUsingUnsafe(lockField, new ReentrantLock(true));
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

    // Line 82~ Line 191 are copied from
    // https://github.com/powermock/powermock/blob/release/2.x/powermock-reflect/src/main/java/org/powermock/reflect/internal/WhiteboxImpl.java
    private static void setField(Object object, Object value, Field foundField) {
        boolean isStatic = (foundField.getModifiers() & Modifier.STATIC) == Modifier.STATIC;
        if (isStatic) {
            setStaticFieldUsingUnsafe(foundField, value);
        } else {
            setFieldUsingUnsafe(foundField, object, value);
        }
    }

    private static void setStaticFieldUsingUnsafe(final Field field, final Object newValue) {
        try {
            field.setAccessible(true);
            int fieldModifiersMask = field.getModifiers();
            boolean isFinalModifierPresent =
                    (fieldModifiersMask & Modifier.FINAL) == Modifier.FINAL;
            if (isFinalModifierPresent) {
                AccessController.doPrivileged(
                        new PrivilegedAction<Object>() {
                            @Override
                            public Object run() {
                                try {
                                    Unsafe unsafe = getUnsafe();
                                    long offset = unsafe.staticFieldOffset(field);
                                    Object base = unsafe.staticFieldBase(field);
                                    setFieldUsingUnsafe(
                                            base, field.getType(), offset, newValue, unsafe);
                                    return null;
                                } catch (Throwable t) {
                                    throw new RuntimeException(t);
                                }
                            }
                        });
            } else {
                field.set(null, newValue);
            }
        } catch (SecurityException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void setFieldUsingUnsafe(
            final Field field, final Object object, final Object newValue) {
        try {
            field.setAccessible(true);
            int fieldModifiersMask = field.getModifiers();
            boolean isFinalModifierPresent =
                    (fieldModifiersMask & Modifier.FINAL) == Modifier.FINAL;
            if (isFinalModifierPresent) {
                AccessController.doPrivileged(
                        new PrivilegedAction<Object>() {
                            @Override
                            public Object run() {
                                try {
                                    Unsafe unsafe = getUnsafe();
                                    long offset = unsafe.objectFieldOffset(field);
                                    setFieldUsingUnsafe(
                                            object, field.getType(), offset, newValue, unsafe);
                                    return null;
                                } catch (Throwable t) {
                                    throw new RuntimeException(t);
                                }
                            }
                        });
            } else {
                try {
                    field.set(object, newValue);
                } catch (IllegalAccessException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } catch (SecurityException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static Unsafe getUnsafe()
            throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
                    SecurityException {
        Field field1 = Unsafe.class.getDeclaredField("theUnsafe");
        field1.setAccessible(true);
        Unsafe unsafe = (Unsafe) field1.get(null);
        return unsafe;
    }

    private static void setFieldUsingUnsafe(
            Object base, Class type, long offset, Object newValue, Unsafe unsafe) {
        if (type == Integer.TYPE) {
            unsafe.putInt(base, offset, ((Integer) newValue));
        } else if (type == Short.TYPE) {
            unsafe.putShort(base, offset, ((Short) newValue));
        } else if (type == Long.TYPE) {
            unsafe.putLong(base, offset, ((Long) newValue));
        } else if (type == Byte.TYPE) {
            unsafe.putByte(base, offset, ((Byte) newValue));
        } else if (type == Boolean.TYPE) {
            unsafe.putBoolean(base, offset, ((Boolean) newValue));
        } else if (type == Float.TYPE) {
            unsafe.putFloat(base, offset, ((Float) newValue));
        } else if (type == Double.TYPE) {
            unsafe.putDouble(base, offset, ((Double) newValue));
        } else if (type == Character.TYPE) {
            unsafe.putChar(base, offset, ((Character) newValue));
        } else {
            unsafe.putObject(base, offset, newValue);
        }
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
