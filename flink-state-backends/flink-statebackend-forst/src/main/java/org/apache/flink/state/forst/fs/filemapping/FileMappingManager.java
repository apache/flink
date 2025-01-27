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
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A manager to manage file mapping of forst file system, including misc file mapping (remote file
 * -> local file) and linked mapping (remote file -> remote file). Note, the key/value of mapping
 * table must be a file path, directories are maintained by file system itself, directories wouldn't
 * be the key/value of mapping table.
 */
public class FileMappingManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileMappingManager.class);

    private final FileSystem fileSystem;

    private final HashMap<String, MappingEntry> mappingTable;

    private final FileOwnershipDecider fileOwnershipDecider;

    private final String remoteBase;
    private final String localBase;

    public FileMappingManager(
            FileSystem fileSystem,
            @Nullable FileOwnershipDecider fileOwnershipDecider,
            String remoteBase,
            String localBase) {
        this.fileSystem = fileSystem;
        this.fileOwnershipDecider =
                fileOwnershipDecider == null
                        ? FileOwnershipDecider.getDefault()
                        : fileOwnershipDecider;
        this.mappingTable = new HashMap<>();
        this.remoteBase = remoteBase;
        this.localBase = localBase;
    }

    /** Create a new file in the mapping table. */
    public MappingEntry createNewFile(Path filePath) {
        String key = filePath.toString();
        if (FileOwnershipDecider.shouldAlwaysBeLocal(filePath)) {
            filePath = forceLocalPath(filePath);
        }

        filePath = toUUIDPath(filePath);

        LOG.trace("decide file ownership for new file: {} {}", filePath, fileOwnershipDecider);
        return addFileToMappingTable(
                key, filePath, fileOwnershipDecider.decideForNewFile(filePath));
    }

    /** Register a file restored from checkpoints to the mapping table. */
    public MappingEntry registerReusedRestoredFile(
            String key, StreamStateHandle stateHandle, Path dbFilePath) {
        // The checkpoint file may contain only the UUID without the file extension, so we：
        //  - Decide file ownership based on dbFilePath, so we can know the real file type.
        //  - Add to mapping table based on cpFilePath, so we can access the real file.
        LOG.trace("decide restored file ownership based on dbFilePath: {}", dbFilePath);
        return addHandleBackedFileToMappingTable(
                key, stateHandle, fileOwnershipDecider.decideForRestoredFile(dbFilePath));
    }

    private MappingEntry addHandleBackedFileToMappingTable(
            String key, StreamStateHandle stateHandle, FileOwnership fileOwnership) {
        MappingEntrySource source = new HandleBackedMappingEntrySource(stateHandle);
        MappingEntry existingEntry = getExistingMappingEntry(key, source, fileOwnership);
        return existingEntry == null
                ? addMappingEntry(key, new MappingEntry(1, source, fileOwnership, false))
                : existingEntry;
    }

    private MappingEntry addFileToMappingTable(
            String key, Path filePath, FileOwnership fileOwnership) {
        MappingEntrySource source = new FileBackedMappingEntrySource(filePath);
        MappingEntry existingEntry = getExistingMappingEntry(key, source, fileOwnership);
        return existingEntry == null
                ? addMappingEntry(key, new MappingEntry(1, filePath, fileOwnership, false))
                : existingEntry;
    }

    private @Nullable MappingEntry getExistingMappingEntry(
            String key, MappingEntrySource source, FileOwnership fileOwnership) {
        MappingEntry entryInTable = mappingTable.getOrDefault(key, null);
        if (entryInTable != null) {
            Preconditions.checkState(
                    entryInTable.source.equals(source)
                            && entryInTable.fileOwnership == fileOwnership,
                    String.format(
                            "Try to add a file that is already in mappingTable,"
                                    + " but with inconsistent entry. Key: %s, source: %s, fileOwnership: %s. "
                                    + " Entry in table: %s",
                            key, source, fileOwnership, entryInTable));

            LOG.trace("Skip adding a file that already exists in mapping table: {}", key);
            return entryInTable;
        }
        return null;
    }

    private MappingEntry addMappingEntry(String key, MappingEntry entry) {
        mappingTable.put(key, entry);
        LOG.trace("Add entry to mapping table: {} -> {}", key, entry);
        return entry;
    }

    /** Add a mapping 'dst -> src' to the mapping table. */
    public int link(String src, String dst) {
        if (src.equals(dst)) {
            return -1;
        }
        // if dst already exist, not allow
        if (mappingTable.containsKey(dst)) {
            return -1;
        }
        MappingEntry sourceEntry = mappingTable.get(src);
        if (sourceEntry == null) {
            throw new RuntimeException(
                    "Unexpected: linking to a file that doesn't exist in ForSt FileMappingManager.");
        }
        sourceEntry.retain();
        mappingTable.putIfAbsent(dst, sourceEntry);
        LOG.trace("link: {} -> {}", dst, src);
        return 0;
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

            LOG.trace("rename: {} -> {}", src, dst);
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
            deleteFileOrDirectory(new Path(src), true);
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
    public boolean deleteFileOrDirectory(Path file, boolean recursive) throws IOException {
        String fileStr = file.toString();
        MappingEntry entry = mappingTable.getOrDefault(fileStr, null);
        LOG.trace("Remove from mapping table: {}, entry:{}", fileStr, entry);
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
        MappingEntry parentEntry =
                new MappingEntry(0, file, FileOwnership.PRIVATE_OWNED_BY_DB, true);

        // step 2.1: find all entries under this directory and set their parentDir to this directory
        for (Map.Entry<String, MappingEntry> currentEntry : mappingTable.entrySet()) {
            MappingEntry mappingEntry = currentEntry.getValue();
            if (!isParentDir(mappingEntry.getSourcePath(), fileStr)) {
                continue;
            }
            MappingEntry oldParentDir = mappingEntry.parentDir;
            if (oldParentDir == null
                    || isParentDir(oldParentDir.getSourcePath(), fileStr)
                            && !oldParentDir.equals(parentEntry)) {
                parentEntry.retain();
                mappingEntry.parentDir = parentEntry;
                // if the file is not owned by DB, set the parentDir to NOT_OWNED
                if (mappingEntry.fileOwnership == FileOwnership.NOT_OWNED) {
                    parentEntry.setFileOwnership(FileOwnership.NOT_OWNED);
                }
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
    public @Nullable MappingEntry mappingEntry(String path) {
        return mappingTable.getOrDefault(path, null);
    }

    private boolean isParentDir(@Nullable Path path, String dir) {
        if (path == null) {
            return false;
        }
        return isParentDir(path.toString(), dir);
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

    public void giveUpOwnership(Path path, StreamStateHandle stateHandle) {
        MappingEntry mappingEntry = mappingTable.getOrDefault(path.toString(), null);
        Preconditions.checkArgument(
                mappingEntry != null,
                "Try to give up ownership of a file that is not in mapping table: %s",
                path);
        Preconditions.checkArgument(
                mappingEntry.fileOwnership != FileOwnership.PRIVATE_OWNED_BY_DB,
                "Try to give up ownership of a file that is not shareable: %s ",
                mappingEntry);

        mappingEntry.setFileOwnership(FileOwnership.NOT_OWNED);
        mappingEntry.setSource(stateHandle);
        LOG.trace(
                "Give up ownership for file: {}, the source is now backed by: {}",
                mappingEntry,
                stateHandle);
    }

    private Path forceLocalPath(Path filePath) {
        if (isParentDir(filePath.toString(), remoteBase)) {
            return new Path(localBase, filePath.getName());
        }
        return filePath;
    }

    private Path toUUIDPath(Path filePath) {
        return new Path(filePath.getParent(), UUID.randomUUID().toString());
    }
}
