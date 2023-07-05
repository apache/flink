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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The partition file with segment file mode. In this mode, each segment of one subpartition is
 * written to an independent file.
 */
public class SegmentPartitionFile {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentPartitionFile.class);

    static final String TIERED_STORAGE_DIR = "tiered-storage";

    static final String SEGMENT_FILE_PREFIX = "seg-";

    static final String SEGMENT_FINISH_DIR_NAME = "FINISH";

    public static SegmentPartitionFileWriter createPartitionFileWriter(
            String dataFilePath, int numSubpartitions) {
        return new SegmentPartitionFileWriter(dataFilePath, numSubpartitions);
    }

    public static SegmentPartitionFileReader createPartitionFileReader(String dataFilePath) {
        return new SegmentPartitionFileReader(dataFilePath);
    }

    // ------------------------------------------------------------------------
    //  File-related utilities
    // ------------------------------------------------------------------------

    public static String getTieredStoragePath(String basePath) {
        return String.format("%s/%s", basePath, TIERED_STORAGE_DIR);
    }

    public static String getPartitionPath(TieredStoragePartitionId partitionId, String basePath) {
        if (basePath == null) {
            return null;
        }

        while (basePath.endsWith("/") && basePath.length() > 1) {
            basePath = basePath.substring(0, basePath.length() - 1);
        }
        return String.format("%s/%s", basePath, TieredStorageIdMappingUtils.convertId(partitionId));
    }

    public static String getSubpartitionPath(
            String basePath, TieredStoragePartitionId partitionId, int subpartitionId) {
        while (basePath.endsWith("/") && basePath.length() > 1) {
            basePath = basePath.substring(0, basePath.length() - 1);
        }
        return String.format(
                "%s/%s/%s",
                basePath, TieredStorageIdMappingUtils.convertId(partitionId), subpartitionId);
    }

    public static Path getSegmentPath(
            String basePath,
            TieredStoragePartitionId partitionId,
            int subpartitionId,
            long segmentId) {
        String subpartitionPath = getSubpartitionPath(basePath, partitionId, subpartitionId);
        return new Path(subpartitionPath, SEGMENT_FILE_PREFIX + segmentId);
    }

    public static Path getSegmentFinishDirPath(
            String basePath, TieredStoragePartitionId partitionId, int subpartitionId) {
        String subpartitionPath = getSubpartitionPath(basePath, partitionId, subpartitionId);
        return new Path(subpartitionPath, SEGMENT_FINISH_DIR_NAME);
    }

    public static void writeBuffers(
            WritableByteChannel writeChannel, long expectedBytes, ByteBuffer[] bufferWithHeaders)
            throws IOException {
        int writeSize = 0;
        for (ByteBuffer bufferWithHeader : bufferWithHeaders) {
            writeSize += writeChannel.write(bufferWithHeader);
        }
        checkState(writeSize == expectedBytes, "Wong number of written bytes.");
    }

    public static void writeSegmentFinishFile(
            String basePath,
            TieredStoragePartitionId partitionId,
            int subpartitionId,
            int segmentId)
            throws IOException {
        Path segmentFinishDir = getSegmentFinishDirPath(basePath, partitionId, subpartitionId);
        FileSystem fs = segmentFinishDir.getFileSystem();
        Path segmentFinishFile = new Path(segmentFinishDir, String.valueOf(segmentId));
        if (!fs.exists(segmentFinishDir)) {
            fs.mkdirs(segmentFinishDir);
            OutputStream outputStream =
                    fs.create(segmentFinishFile, FileSystem.WriteMode.OVERWRITE);
            outputStream.close();
            return;
        }

        FileStatus[] files = fs.listStatus(segmentFinishDir);
        if (files.length == 0) {
            OutputStream outputStream =
                    fs.create(segmentFinishFile, FileSystem.WriteMode.OVERWRITE);
            outputStream.close();
        } else {
            // To minimize the number of files, each subpartition keeps only a single segment-finish
            // file. For instance, if segment-finish file 5 exists, it indicates that segments 1 to
            // 5 have all been finished.
            // Note that this check requires the file system to ensure that only one file is in the
            // directory can be accessed when renaming a file.
            checkState(files.length == 1, "Wong number of segment-finish files.");
            fs.rename(files[0].getPath(), segmentFinishFile);
        }
    }

    public static void deletePathQuietly(String toDelete) {
        try {
            Path toRemovePath = new Path(toDelete);
            FileSystem fs = toRemovePath.getFileSystem();
            if (fs.exists(toRemovePath)) {
                fs.delete(toRemovePath, true);
            }
        } catch (IOException e) {
            LOG.error("Failed to delete files for {} ", toDelete, e);
        }
    }
}
