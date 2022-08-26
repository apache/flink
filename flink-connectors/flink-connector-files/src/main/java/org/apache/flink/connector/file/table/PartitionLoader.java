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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;
import static org.apache.flink.table.utils.PartitionPathUtils.listStatusWithoutHidden;

/**
 * Loader to temporary files to final output path and meta store. According to overwrite, the loader
 * will delete the previous data.
 *
 * <p>This provide two interface to load: 1.{@link #loadPartition}: load temporary partitioned
 * files, if it is new partition, will create partition to meta store. 2.{@link #loadNonPartition}:
 * just rename all files to final output path.
 *
 * <p>TODO: src and dest may be on different FS.
 */
@Internal
public class PartitionLoader implements Closeable {

    private final boolean overwrite;
    private final FileSystem fs;
    private final TableMetaStoreFactory.TableMetaStore metaStore;
    // whether it's to load files to local file system
    private final boolean isToLocal;
    private final ObjectIdentifier identifier;
    private final List<PartitionCommitPolicy> policies;

    public PartitionLoader(
            boolean overwrite,
            FileSystem sourceFs,
            TableMetaStoreFactory factory,
            boolean isToLocal,
            ObjectIdentifier identifier,
            List<PartitionCommitPolicy> policies)
            throws Exception {
        this.overwrite = overwrite;
        this.fs = sourceFs;
        this.metaStore = factory.createTableMetaStore();
        this.isToLocal = isToLocal;
        this.identifier = identifier;
        this.policies = policies;
    }

    /** Load a single partition. */
    public void loadPartition(LinkedHashMap<String, String> partSpec, List<Path> srcDirs)
            throws Exception {
        Optional<Path> pathFromMeta = metaStore.getPartition(partSpec);
        Path path =
                pathFromMeta.orElseGet(
                        () ->
                                new Path(
                                        metaStore.getLocationPath(),
                                        generatePartitionPath(partSpec)));

        overwriteAndMoveFiles(srcDirs, path);
        commitPartition(partSpec, path);
    }

    /** Load a non-partition files to output path. */
    public void loadNonPartition(List<Path> srcDirs) throws Exception {
        Path tableLocation = metaStore.getLocationPath();
        overwriteAndMoveFiles(srcDirs, tableLocation);
        commitPartition(new LinkedHashMap<>(), tableLocation);
        metaStore.finishWritingTable(tableLocation);
    }

    /**
     * The flink job does not write data to the partition, but the corresponding partition needs to
     * be created or updated.
     *
     * <p>The partition does not exist, create it.
     *
     * <p>The partition exists:
     *
     * <pre>
     *      if overwrite is true, delete the path, then create it;
     *      if overwrite is false, do nothing;
     * </pre>
     */
    public void loadEmptyPartition(LinkedHashMap<String, String> partSpec) throws Exception {
        Optional<Path> pathFromMeta = metaStore.getPartition(partSpec);
        if (pathFromMeta.isPresent() && !overwrite) {
            commitPartition(partSpec, pathFromMeta.get());
            return;
        }
        Path path = new Path(metaStore.getLocationPath(), generatePartitionPath(partSpec));
        if (pathFromMeta.isPresent()) {
            fs.delete(pathFromMeta.get(), true);
            fs.mkdirs(path);
        }
        commitPartition(partSpec, path);
    }

    private void overwriteAndMoveFiles(List<Path> srcDirs, Path destDir) throws Exception {
        FileSystem destFileSystem = destDir.getFileSystem();
        boolean dirSuccessExist = destFileSystem.exists(destDir) || destFileSystem.mkdirs(destDir);
        Preconditions.checkState(dirSuccessExist, "Failed to create dest path " + destDir);
        overwrite(destDir);
        moveFiles(srcDirs, destDir);
    }

    private void overwrite(Path destDir) throws Exception {
        if (overwrite) {
            // delete existing files for overwrite
            FileStatus[] existingFiles = listStatusWithoutHidden(destDir.getFileSystem(), destDir);
            if (existingFiles != null) {
                for (FileStatus existingFile : existingFiles) {
                    // TODO: We need move to trash when auto-purge is false.
                    fs.delete(existingFile.getPath(), true);
                }
            }
        }
    }

    /** Moves files from srcDir to destDir. */
    private void moveFiles(List<Path> srcDirs, Path destDir) throws Exception {
        for (Path srcDir : srcDirs) {
            if (!srcDir.equals(destDir)) {
                FileStatus[] srcFiles = listStatusWithoutHidden(fs, srcDir);
                if (srcFiles != null) {
                    for (FileStatus srcFile : srcFiles) {
                        Path srcPath = srcFile.getPath();
                        Path destPath = new Path(destDir, srcPath.getName());
                        // if it's not to move to local file system, just rename it
                        if (!isToLocal) {
                            fs.rename(srcPath, destPath);
                        } else {
                            FileUtils.copy(srcPath, destPath, true);
                        }
                    }
                }
            }
        }
    }

    /**
     * Reuse of PartitionCommitPolicy mechanisms. The default in Batch mode is metastore and
     * success-file.
     */
    private void commitPartition(LinkedHashMap<String, String> partitionSpec, Path path)
            throws Exception {
        PartitionCommitPolicy.Context context = new CommitPolicyContextImpl(partitionSpec, path);
        for (PartitionCommitPolicy policy : policies) {
            if (policy instanceof MetastoreCommitPolicy) {
                if (partitionSpec.isEmpty()) {
                    // Non partition table skip commit meta data.
                    continue;
                }
                ((MetastoreCommitPolicy) policy).setMetastore(metaStore);
            }
            policy.commit(context);
        }
    }

    @Override
    public void close() throws IOException {
        metaStore.close();
    }

    private class CommitPolicyContextImpl implements PartitionCommitPolicy.Context {

        private final Path partitionPath;
        private final LinkedHashMap<String, String> partitionSpec;

        private CommitPolicyContextImpl(
                LinkedHashMap<String, String> partitionSpec, Path partitionPath) {
            this.partitionSpec = partitionSpec;
            this.partitionPath = partitionPath;
        }

        @Override
        public String catalogName() {
            return identifier.getCatalogName();
        }

        @Override
        public String databaseName() {
            return identifier.getDatabaseName();
        }

        @Override
        public String tableName() {
            return identifier.getObjectName();
        }

        @Override
        public List<String> partitionKeys() {
            List<String> partitionKeys = new LinkedList<>();
            for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
                partitionKeys.add(entry.getKey());
            }
            return partitionKeys;
        }

        @Override
        public List<String> partitionValues() {
            return new ArrayList<>(partitionSpec.values());
        }

        @Override
        public Path partitionPath() {
            return this.partitionPath;
        }

        @Override
        public LinkedHashMap<String, String> partitionSpec() {
            return partitionSpec;
        }
    }
}
