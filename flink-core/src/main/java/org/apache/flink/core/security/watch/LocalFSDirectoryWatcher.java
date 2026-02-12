/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.security.watch;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.Map;
import java.util.Set;

/**
 * Interface for watching file system directories and notifying listeners of changes.
 *
 * <p>Implementations monitor directories for file modifications and invoke registered {@link
 * LocalFSWatchServiceListener} callbacks when changes occur.
 */
public interface LocalFSDirectoryWatcher {

    /**
     * Returns the current set of registered watchers.
     *
     * @return an immutable set of watch service and listener pairs
     */
    Set<Map.Entry<WatchService, LocalFSWatchServiceListener>> getWatchers();

    /**
     * Registers directories for monitoring and associates them with a listener.
     *
     * <p>The listener will be notified when files within these directories are created, modified,
     * or deleted.
     *
     * @param dirsToWatch the directories to watch (must be directories, not files)
     * @param listener the listener to notify when changes occur in the watched directories
     * @throws IOException if an I/O error occurs or if a file (not directory) is provided
     */
    void registerDirectory(Path[] dirsToWatch, LocalFSWatchServiceListener listener)
            throws IOException;
}
