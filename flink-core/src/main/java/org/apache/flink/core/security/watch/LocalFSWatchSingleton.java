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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public final class LocalFSWatchSingleton {
    // The field must be declared volatile so that double check lock would work
    // correctly.
    private static volatile LocalFSWatchSingleton instance;

    ConcurrentHashMap<WatchService, LocalFSWatchServiceListener> watchers =
            new ConcurrentHashMap<>();

    private LocalFSWatchSingleton() {}

    public static LocalFSWatchSingleton getInstance() {
        LocalFSWatchSingleton result = instance;
        if (result != null) {
            return result;
        }
        synchronized (LocalFSWatchSingleton.class) {
            if (instance == null) {
                instance = new LocalFSWatchSingleton();
            }
            return instance;
        }
    }

    public void registerPath(Path[] pathsToWatch, LocalFSWatchServiceListener callback)
            throws IOException {

        WatchService watcher = FileSystems.getDefault().newWatchService();
        for (Path pathToWatch : pathsToWatch) {
            Path realDirectoryPath = pathToWatch.toRealPath();
            realDirectoryPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        }
        callback.onWatchStarted(pathsToWatch[0]);
        watchers.put(watcher, callback);
    }
}
