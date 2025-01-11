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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A manager to manage file mapping of forst file system, including misc file mapping (remote file
 * -> local file) and linked mapping (remote file -> remote file). Note, the key/value of mapping
 * table must be a file path, directories are maintained by file system itself, directories wouldn't
 * be the key/value of mapping table.
 */
public class FileMappingManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileMappingManager.class);

    public static final String SST_SUFFIX = ".sst";

    private final FileSystem fileSystem;

    private final FileSystem localFileSystem;

    private final HashMap<String, MappingEntry> mappingTable;

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

    /** Create a mapping entry for a file. */
    public RealPath createFile(Path file) {
        String fileName = file.toString();
        Preconditions.checkState(!mappingTable.containsKey(fileName));
        if (!fileName.endsWith(SST_SUFFIX) && isParentDir(fileName, remoteBase)) {
            Path localFile = new Path(localBase, file.getName());
            mappingTable.put(
                    fileName,
                    new MappingEntry(1, localFileSystem, localFile.toString(), true, false));
            return new RealPath(localFile, true);
        } else {
            mappingTable.put(fileName, new MappingEntry(1, fileSystem, fileName, false, false));
            return new RealPath(file, false);
        }
    }

    /** Called by link/copy. Directory link is not supported now. */
    public int link(String src, String dst) {
        if (src.equals(dst)) {
            return -1;
        }
        // if dst already exist, not allow
        if (mappingTable.containsKey(dst)) {
            return -1;
        }
        MappingEntry sourceEntry = mappingTable.get(src);
        Preconditions.checkNotNull(sourceEntry);
        sourceEntry.retain();
        mappingTable.putIfAbsent(dst, sourceEntry);
        LOG.trace("link: {} -> {}", dst, src);
        return 0;
    }

    /**
     * Get the real path of a file, the real path maybe a local file or a remote file/dir. Due to
     * the lazy deletion, if the path is a directory, the exists check may have false positives.
     */
    public RealPath realPath(Path path) {
        String fileName = path.toString();
        MappingEntry entry = mappingTable.getOrDefault(fileName, null);
        if (entry != null) {
            return new RealPath(new Path(entry.sourcePath), entry.isLocal);
        }
        return null;
    }

    public List<String> listByPrefix(String path) {
        List<String> linkedPaths = new ArrayList<>();
        for (Map.Entry<String, MappingEntry> entry : mappingTable.entrySet()) {
            if (isParentDir(entry.getKey(), path)) {
                linkedPaths.add(entry.getKey());
            }
        }
        return linkedPaths;
    }

    /**
     * 1. If src can match any key, we only `mark rename`, no physical file would be renamed. 2. If
     * src is a directory, all files under src will be renamed, including linked files and local
     * files, the directory also would be renamed in file system physically.
     *
     * @param src the source path
     * @param dst the destination path
     * @return always return true except for IOException
     */
    public boolean renameFile(String src, String dst) throws IOException {
        MappingEntry srcEntry = mappingTable.get(src);
        if (srcEntry != null) { // rename file
            if (mappingTable.containsKey(dst)) {
                MappingEntry dstEntry = mappingTable.remove(dst);
                dstEntry.release();
            }
            mappingTable.remove(src);
            mappingTable.put(dst, srcEntry);
        } else { // rename directory = link to dst dir + delete src dir

            // step 1: link all files under src to dst
            List<String> toRename = listByPrefix(src);
            for (String key : toRename) {
                MappingEntry sourceEntry = mappingTable.get(key);
                sourceEntry.retain();
                String renamedDst = key.replace(src, dst);
                LOG.trace("rename: {} -> {}", key, renamedDst);
                mappingTable.put(renamedDst, sourceEntry);
            }

            Path dstPath = new Path(dst);
            if (!fileSystem.exists(dstPath)) {
                fileSystem.mkdirs(dstPath);
            }
            // step 2: delete src dir
            deleteFile(new Path(src), true);
        }
        return true;
    }

    /**
     * Delete a file or directory from mapping table and file system, the directory deletion may be
     * deferred.
     *
     * @param file to be deleted
     * @param recursive whether to delete recursively
     * @return true if the file or directory is deleted successfully, false otherwise.
     * @throws IOException if an error occurs during deletion
     */
    public boolean deleteFile(Path file, boolean recursive) throws IOException {
        String fileStr = file.toString();
        MappingEntry entry = mappingTable.getOrDefault(fileStr, null);
        LOG.trace("delete: {}, source:{}", file, entry == null ? "null" : entry.sourcePath);
        // case 1: delete file
        if (entry != null) {
            mappingTable.remove(fileStr);
            entry.release();
            return true;
        }

        // case 2: delete directory
        if (!recursive) {
            throw new IOException(fileStr + "is a directory, delete failed.");
        }
        MappingEntry parentEntry = new MappingEntry(0, fileSystem, fileStr, false, recursive);

        // step 2.1: find all matched entries, mark delete dir as parent dir
        for (Map.Entry<String, MappingEntry> currentEntry : mappingTable.entrySet()) {
            if (!isParentDir(currentEntry.getValue().sourcePath, fileStr)) {
                continue;
            }
            MappingEntry oldParentDir = currentEntry.getValue().parentDir;
            if (oldParentDir == null
                    || isParentDir(oldParentDir.sourcePath, fileStr)
                            && !oldParentDir.equals(parentEntry)) {
                parentEntry.retain();
                currentEntry.getValue().parentDir = parentEntry;
            }
        }

        boolean status = true;
        // step 2.2: release file under directory
        if (parentEntry.getReferenceCount() == 0) {
            // an empty directory
            status = fileSystem.delete(file, recursive);
        }
        List<String> toRelease = listByPrefix(fileStr);
        for (String key : toRelease) {
            mappingTable.remove(key).release();
        }
        return status;
    }

    @VisibleForTesting
    public MappingEntry mappingEntry(String path) {
        return mappingTable.getOrDefault(path, null);
    }

    private boolean isParentDir(String path, String dir) {
        if (dir.isEmpty()) {
            return false;
        }
        if (dir.charAt(dir.length() - 1) == '/') {
            return path.startsWith(dir);
        } else {
            return (path.startsWith(dir + "/"));
        }
    }

    /** A wrapper of real path. */
    public static class RealPath {
        public final Path path;
        public final boolean isLocal;

        public RealPath(Path path, boolean isLocal) {
            this.path = path;
            this.isLocal = isLocal;
        }
    }
}
