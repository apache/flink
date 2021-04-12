/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.batch.tests.util;

import com.sun.nio.file.SensitivityWatchEventModifier;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A synchronization aid that allows a single thread to wait on the creation of a specified file.
 */
@NotThreadSafe
public class FileBasedOneShotLatch implements Closeable {

    private final Path latchFile;

    private final WatchService watchService;

    private boolean released;

    public FileBasedOneShotLatch(final Path latchFile) {
        this.latchFile = checkNotNull(latchFile);

        final Path parentDir = checkNotNull(latchFile.getParent(), "latchFile must have a parent");
        this.watchService = initWatchService(parentDir);
    }

    private static WatchService initWatchService(final Path parentDir) {
        final WatchService watchService = createWatchService(parentDir);
        watchForLatchFile(watchService, parentDir);
        return watchService;
    }

    private static WatchService createWatchService(final Path parentDir) {
        try {
            return parentDir.getFileSystem().newWatchService();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void watchForLatchFile(final WatchService watchService, final Path parentDir) {
        try {
            parentDir.register(
                    watchService,
                    new WatchEvent.Kind[] {StandardWatchEventKinds.ENTRY_CREATE},
                    SensitivityWatchEventModifier.HIGH);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Waits until the latch file is created.
     *
     * <p>When this method returns, subsequent invocations will not block even after the latch file
     * is deleted. Note that this method may not return if the latch file is deleted before this
     * method returns.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    public void await() throws InterruptedException {
        if (isReleasedOrReleasable()) {
            return;
        }

        awaitLatchFile(watchService);
    }

    private void awaitLatchFile(final WatchService watchService) throws InterruptedException {
        while (true) {
            WatchKey watchKey = watchService.take();
            if (isReleasedOrReleasable()) {
                break;
            }
            watchKey.reset();
        }
    }

    private boolean isReleasedOrReleasable() {
        if (released) {
            return true;
        }

        if (Files.exists(latchFile)) {
            releaseLatch();
            return true;
        }

        return false;
    }

    private void releaseLatch() {
        released = true;
    }

    @Override
    public void close() throws IOException {
        watchService.close();
    }
}
