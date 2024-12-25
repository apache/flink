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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A manager to manage file mapping of forst file system, including misc file mapping (remote file
 * -> local file) and linked mapping (remote file -> remote file). And linked mapping will always be
 * sst file. Directory link is not support now.
 */
public class FileMappingManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileMappingManager.class);

    public static final String SST_SUFFIX = ".sst";

    private FileSystem fileSystem;

    private FileSystem localFileSystem;

    private HashMap<String, MappingEntry> mappingTable;

    private final String remoteBase;

    private final String localBase;

    public FileMappingManager(
            FileSystem fileSystem,
            FileSystem localFileSystem,
            String remoteBase,
            String localBase) {
        this.fileSystem = fileSystem;
        this.localFileSystem = localFileSystem;
        this.mappingTable = new HashMap<>();
        this.remoteBase = remoteBase;
        this.localBase = localBase;
    }

    /** Called by link/copy. Directory link is not supported now. */
    public int put(String src, String dst) {
        if (src == dst) { // self link is not supported
            return -1;
        }
        // if dst already exist, not allow
        if (mappingTable.containsKey(dst)) {
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

    public boolean mkdir(Path path) {
        String dirname = path.toString();
        MappingEntry entry = mappingTable.getOrDefault(dirname, null);
        if (entry != null) {
            return true;
        }
        mappingTable.put(dirname, new MappingEntry(1, fileSystem, dirname, true));
        return true;
    }

    public RealPath realPath(Path file, boolean update) {
        String fileName = file.toString();
        MappingEntry entry = mappingTable.getOrDefault(fileName, null);
        if (!fileName.endsWith(SST_SUFFIX) && fileName.startsWith(remoteBase)) {
            if (entry != null) {
                return new RealPath(new Path(entry.sourcePath), true, entry.isDirectory);
            }
            Path localFile = new Path(localBase, file.getName());
            if (update) {
                mappingTable.put(
                        fileName,
                        new MappingEntry(1, localFileSystem, localFile.toString(), false));
            }
            return new RealPath(localFile, true, false);
        }
        if (entry != null) {
            return new RealPath(new Path(entry.sourcePath), false, entry.isDirectory);
        }
        return new RealPath(file, false, false);
    }

    public List<String> listByPrefix(String path) {
        List<String> linkedPaths = new ArrayList<>();
        for (Map.Entry<String, MappingEntry> entry : mappingTable.entrySet()) {
            if (entry.getKey().startsWith(path) && !entry.getValue().isDirectory) {
                linkedPaths.add(entry.getKey());
            }
        }
        return linkedPaths;
    }

    /**
     * 1. If src can match any key, we only `mark rename`, no physical file would be renamed. 2. If
     * src is a directory, all files under src will be renamed, including linked files and local
     * files. 3. If src can't match any key, we will rename the file/directory.
     *
     * @param src the source path
     * @param dst the destination path
     * @return true if it has renamed in mappingTable, false otherwise.
     */
    public boolean renameFile(String src, String dst) {
        List<String> toRename = new ArrayList<>();
        for (String key : mappingTable.keySet()) {
            if (key.equals(src)) {
                toRename.add(key);
            } else if (isParentDir(key, src)) {
                toRename.add(key);
            }
        }
        if (toRename.size() > 0) {
            for (String key : toRename) {
                MappingEntry sourceEntry = mappingTable.remove(key);
                String renamedDst = key.replace(src, dst);
                LOG.trace("rename: {} -> {}", key, renamedDst);
                mappingTable.put(renamedDst, sourceEntry);
            }
            return true;
        }
        return false;
    }

    /**
     * @param file to delete
     * @return status code: 1: deleted from mappingTable. 0: file not exist. -1: file exist, but not
     *     in mappingTable.
     * @throws IOException
     */
    public boolean deleteFile(Path file) throws IOException {
        String fileStr = file.toString();
        MappingEntry entry = mappingTable.getOrDefault(fileStr, null);
        LOG.trace("delete: {}, source:{}", file, entry == null ? "" : entry.sourcePath);
        if (entry != null && !entry.isDirectory) {
            entry.release();
            mappingTable.remove(fileStr);
            return true;
        }

        if (!fileSystem.exists(file)) {
            return true;
        }

        FileStatus fileStatus = fileSystem.getFileStatus(file);
        if (!fileStatus.isDir()) {
            return false;
        }

        MappingEntry parentEntry = new MappingEntry(0, fileSystem, fileStr, true);

        boolean matched = false;
        for (MappingEntry sourceEntry : mappingTable.values()) {
            if (sourceEntry.sourcePath.startsWith(fileStr)) {
                parentEntry.retain();
                sourceEntry.parentDir = parentEntry;
                matched = true;
            }
        }
        return matched;
    }

    @VisibleForTesting
    public MappingEntry mappingEntry(String path) {
        return mappingTable.getOrDefault(path, null);
    }

    @VisibleForTesting
    public RealPath realPath(Path file) {
        return realPath(file, false);
    }

    private boolean isParentDir(String path, String dir) {
        if (dir.length() == 0) {
            return false;
        }
        if (dir.charAt(dir.length() - 1) == '/') {
            return path.startsWith(dir);
        } else if (path.startsWith(dir + "/")) {
            return true;
        }
        return false;
    }

    /** A wrapper of real path. */
    public class RealPath {
        public final Path path;
        public final boolean isLocal;
        public final boolean isDir;

        public RealPath(Path path, boolean isLocal, boolean isDir) {
            this.path = path;
            this.isLocal = isLocal;
            this.isDir = isDir;
        }
    }
}
