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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public final class LocalFSWatchSingleton implements LocalFSDirectoryWatcher {
    // The field must be declared volatile so that double check lock would work
    // correctly.
    private static volatile LocalFSDirectoryWatcher instance;

    ConcurrentHashMap<WatchService, LocalFSWatchServiceListener> watchers =
            new ConcurrentHashMap<>();

    private LocalFSWatchSingleton() {}

    public static LocalFSDirectoryWatcher getInstance() {
        LocalFSDirectoryWatcher result = instance;
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

    public Set<Map.Entry<WatchService, LocalFSWatchServiceListener>> getWatchers() {
        return watchers.entrySet();
    }

    @Override
    public void registerDirectory(Path[] dirsToWatch, LocalFSWatchServiceListener listener)
            throws IOException {

        WatchService watcher = FileSystems.getDefault().newWatchService();
        for (Path pathToWatch : dirsToWatch) {
            Path realDirectoryPath = pathToWatch.toRealPath();
            realDirectoryPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        }
        listener.onWatchStarted(dirsToWatch[0]);
        watchers.put(watcher, listener);
    }
}
