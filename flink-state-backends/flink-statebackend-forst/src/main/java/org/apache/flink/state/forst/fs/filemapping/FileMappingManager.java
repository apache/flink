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

package org.apache.flink.state.forst.fs.filemapping;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A manager to manage file mapping of forst file system. Only interact with
 * copy()/link()/exist()/delete()/list(), create() won't use file mapping.
 */
public class FileMappingManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileMappingManager.class);

    private FileSystem fileSystem;

    private HashMap<String, MappingEntry> mappingTable;

    public FileMappingManager(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
        this.mappingTable = new HashMap<>();
    }

    /** Called by link/copy. */
    public int put(String src, String dst) {
        if (src == dst) { // self link is not supported
            return -1;
        }
        MappingEntry sourceEntry = mappingTable.get(src);
        if (sourceEntry != null) {
            sourceEntry.retain();
            mappingTable.putIfAbsent(dst, sourceEntry);
        } else {
            sourceEntry = new MappingEntry(fileSystem, src);
            mappingTable.put(src, sourceEntry);
            mappingTable.put(dst, sourceEntry);
        }
        LOG.trace("put: {} -> {}", src, dst);
        return 0;
    }

    public String originalPath(String fileName) {
        MappingEntry entry = mappingTable.getOrDefault(fileName, null);
        if (entry != null) {
            return entry.sourcePath;
        }
        return fileName;
    }

    public List<String> listByPrefix(String path) {
        List<String> linkedPaths = new ArrayList<>();
        for (String key : mappingTable.keySet()) {
            if (key.startsWith(path)) {
                linkedPaths.add(key);
            }
        }
        return linkedPaths;
    }

    public void renameFile(String src, String dst) {
        List<String> toRename = new ArrayList<>();
        for (String key : mappingTable.keySet()) {
            if (key.startsWith(src)) {
                toRename.add(key);
            }
            MappingEntry sourceEntry = mappingTable.get(key);
            if (sourceEntry.sourcePath.startsWith(src)) {
                sourceEntry.sourcePath = sourceEntry.sourcePath.replace(src, dst);
            }
        }
        if (toRename.size() > 0) {
            for (String key : toRename) {
                MappingEntry sourceEntry = mappingTable.remove(key);
                String renamedDst = key.replace(src, dst);
                LOG.trace("rename: {} -> {}", key, renamedDst);
                mappingTable.put(renamedDst, sourceEntry);
            }
        }
    }

    public boolean deleteFile(Path file, boolean recursive) throws IOException {
        MappingEntry entry = mappingTable.getOrDefault(file.toString(), null);
        LOG.trace("delete: {}, source:{}", file, entry == null ? "" : entry.sourcePath);
        if (entry != null) {
            entry.release();
            mappingTable.remove(file.toString());
            return true;
        }
        boolean matchedDir =
                mappingTable.keySet().stream().anyMatch(key -> key.startsWith(file.toString()));
        if (!matchedDir && !fileSystem.exists(file)) {
            return false;
        }
        if (!matchedDir) {
            FileStatus fileStatus = fileSystem.getFileStatus(file);
            if (!fileStatus.isDir()) {
                return fileSystem.delete(file, recursive);
            }
        }

        String parentDir = file.toString();
        Preconditions.checkState(!mappingTable.containsKey(parentDir));
        int initRefCount =
                mappingTable.values().stream()
                        .mapToInt(src -> (src.sourcePath.startsWith(parentDir) ? 1 : 0))
                        .sum();
        MappingEntry parentEntry = new MappingEntry(initRefCount, fileSystem, parentDir, true);
        for (MappingEntry sourceEntry : mappingTable.values()) {
            if (sourceEntry.sourcePath.startsWith(parentDir)) {
                sourceEntry.parentDir = parentEntry;
            }
        }
        return true;
    }

    @VisibleForTesting
    public MappingEntry mappingEntry(String path) {
        return mappingTable.getOrDefault(path, null);
    }
}
