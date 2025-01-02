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
        if (!fileName.endsWith(SST_SUFFIX) && fileName.startsWith(remoteBase)) {
            Path localFile = new Path(localBase, file.getName());
            mappingTable.put(
                    fileName, new MappingEntry(1, localFileSystem, localFile.toString(), false));
            return new RealPath(localFile, true);
        } else {
            mappingTable.put(fileName, new MappingEntry(1, fileSystem, fileName, false));
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
        if (sourceEntry != null) {
            sourceEntry.retain();
            mappingTable.putIfAbsent(dst, sourceEntry);
        } else {
            sourceEntry = new MappingEntry(0, fileSystem, src, false);
            sourceEntry.retain();
            mappingTable.put(src, sourceEntry);
            sourceEntry.retain();
            mappingTable.put(dst, sourceEntry);
        }
        LOG.trace("link: {} -> {}", dst, src);
        return 0;
    }

    /** Get the real path of a file, the real path maybe a local file or a remote file/dir. */
    public RealPath realPath(Path file) {
        String fileName = file.toString();
        MappingEntry entry = mappingTable.getOrDefault(fileName, null);
        if (!fileName.endsWith(SST_SUFFIX) && fileName.startsWith(remoteBase)) {
            if (entry != null) {
                return new RealPath(new Path(entry.sourcePath), true);
            }
            Path localFile = new Path(localBase, file.getName());
            return new RealPath(localFile, true);
        }
        if (entry != null) {
            return new RealPath(new Path(entry.sourcePath), false);
        }
        return new RealPath(file, false);
    }

    public List<String> listByPrefix(String path) {
        List<String> linkedPaths = new ArrayList<>();
        for (Map.Entry<String, MappingEntry> entry : mappingTable.entrySet()) {
            if (entry.getKey().startsWith(path) && !entry.getValue().recursive) {
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
        } else { // rename directory
            // 1. rename file in old directory
            List<String> toRename = new ArrayList<>();
            for (String key : mappingTable.keySet()) {
                if (isParentDir(key, src) || key.equals(src)) {
                    toRename.add(key);
                }
            }
            if (!toRename.isEmpty()) {
                for (String key : toRename) {
                    MappingEntry sourceEntry = mappingTable.remove(key);
                    String renamedDst = key.replace(src, dst);
                    LOG.trace("rename: {} -> {}", key, renamedDst);
                    sourceEntry.sourcePath = sourceEntry.sourcePath.replace(src, dst);
                    mappingTable.put(renamedDst, sourceEntry);
                }
            }
            // 2. rename directory in file system physically
            Path srcPath = new Path(src);
            Path dstPath = new Path(dst);

            // The rename is not atomic for ForSt. Some FileSystems e.g. HDFS, OSS does not allow a
            // renaming if the target already exists. So, we delete the target before attempting the
            // rename.
            if (fileSystem.exists(dstPath)) {
                boolean deleted = fileSystem.delete(dstPath, true);
                if (!deleted) {
                    throw new IOException("Fail to rename path, src:" + src + ", dst:" + dst);
                }
            }
            fileSystem.rename(srcPath, dstPath);
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
     * @throws IOException
     */
    public boolean deleteFile(Path file, boolean recursive) throws IOException {
        String fileStr = file.toString();
        MappingEntry entry = mappingTable.getOrDefault(fileStr, null);
        LOG.trace("delete: {}, source:{}", file, entry == null ? "null" : entry.sourcePath);
        if (entry != null) { // delete file
            mappingTable.remove(fileStr);
            entry.release();
            return true;
        }

        // delete directory
        MappingEntry parentEntry = new MappingEntry(0, fileSystem, fileStr, recursive);

        boolean matched = false;
        for (MappingEntry mappingEntry : mappingTable.values()) {
            if (isParentDir(mappingEntry.sourcePath, fileStr)) {
                parentEntry.retain();
                mappingEntry.parentDir = parentEntry;
                matched = true;
            }
        }
        if (!matched) {
            return fileSystem.delete(file, recursive);
        }
        return true;
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
