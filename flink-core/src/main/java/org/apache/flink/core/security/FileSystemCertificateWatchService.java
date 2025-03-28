/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/** Service which is able to watch local filesystem directories. */
public class FileSystemCertificateWatchService extends Thread {

    private static final Logger LOG =
            LoggerFactory.getLogger(FileSystemCertificateWatchService.class);

    private final Set<String> directoryPaths;
    private final Callable<Void> onModifiedCallable;

    public FileSystemCertificateWatchService(
            Set<String> directoryPaths, Callable<Void> onModifiedCallable) {
        for (String directoryPath : directoryPaths) {
            if (!new File(directoryPath).isDirectory()) {
                throw new IllegalArgumentException("Directory must exists: " + directoryPath);
            }
        }
        this.directoryPaths = directoryPaths;
        this.onModifiedCallable = onModifiedCallable;
    }

    public void launch() {
        this.setDaemon(true);
        this.start();
    }

    @Override
    public void run() {
        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            for (String directoryPath : directoryPaths) {
                LOG.info("Starting watching path: {}", directoryPath);
                Path realDirectoryPath = Paths.get(directoryPath).toRealPath();
                LOG.info("Path is resolved to real path: {}", realDirectoryPath);
                realDirectoryPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
                onWatchStarted(realDirectoryPath);
            }

            while (true) {
                LOG.debug("Taking watch key");
                WatchKey watchKey = watcher.take();
                LOG.debug("Watch key arrived");
                for (WatchEvent<?> watchEvent : watchKey.pollEvents()) {
                    if (watchEvent.kind() == OVERFLOW) {
                        LOG.error("Filesystem events may have been lost or discarded");
                        Thread.yield();
                    } else if (watchEvent.kind() == ENTRY_CREATE) {
                        onFileOrDirectoryCreated((Path) watchEvent.context());
                    } else if (watchEvent.kind() == ENTRY_DELETE) {
                        onFileOrDirectoryDeleted((Path) watchEvent.context());
                    } else if (watchEvent.kind() == ENTRY_MODIFY) {
                        onFileOrDirectoryModified((Path) watchEvent.context());
                    } else {
                        LOG.warn("Unhandled watch event {}", watchEvent.kind());
                    }
                }
                watchKey.reset();
            }
        } catch (InterruptedException e) {
            LOG.info("Filesystem watcher interrupted");
        } catch (Exception e) {
            LOG.error("Filesystem watcher received exception and stopped: ", e);
            throw new RuntimeException(e);
        }
    }

    protected void onWatchStarted(Path realDirectoryPath) {}

    private void onFileOrDirectoryCreated(Path relativePath) {}

    private void onFileOrDirectoryDeleted(Path relativePath) {}

    private void onFileOrDirectoryModified(Path relativePath) {
        try {
            LOG.info("Reloading SSL context because {} has been modified", relativePath);
            onModifiedCallable.call();
        } catch (Exception e) {
            LOG.error("Failed to reload SSL context because {} has been modified", relativePath);
        }
    }
}
