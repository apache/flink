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

package org.apache.flink.core.security.watch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Map;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

public class LocalFSWatchService extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFSWatchService.class);

    public void run() {
        try {
            while (true) {
                for (Map.Entry<WatchService, LocalFSWatchServiceListener> entry :
                        LocalFSWatchSingleton.getInstance().watchers.entrySet()) {
                    LOG.debug("Taking watch key");
                    WatchKey watchKey = entry.getKey().poll();
                    if (watchKey == null) {
                        continue;
                    }
                    LOG.debug("Watch key arrived");
                    for (WatchEvent<?> watchEvent : watchKey.pollEvents()) {
                        System.out.println(watchEvent.kind());
                        System.out.println(watchEvent.context());
                        if (watchEvent.kind() == OVERFLOW) {
                            LOG.error("Filesystem events may have been lost or discarded");
                            Thread.yield();
                        } else if (watchEvent.kind() == ENTRY_CREATE) {
                            entry.getValue().onFileOrDirectoryCreated((Path) watchEvent.context());
                        } else if (watchEvent.kind() == ENTRY_DELETE) {
                            entry.getValue().onFileOrDirectoryDeleted((Path) watchEvent.context());
                        } else if (watchEvent.kind() == ENTRY_MODIFY) {
                            entry.getValue().onFileOrDirectoryModified((Path) watchEvent.context());
                        } else {
                            LOG.warn("Unhandled watch event {}", watchEvent.kind());
                        }
                    }
                    watchKey.reset();
                }
            }
        } catch (Exception e) {
            LOG.error("Filesystem watcher received exception and stopped: ", e);
            throw new RuntimeException(e);
        }
    }
}
