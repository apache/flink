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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.getSegmentFinishDirPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.getSegmentPath;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link RemoteStorageScanner} is introduced to notify asynchronously for file reading on
 * remote storage. Asynchronous notifications will prevent {@link RemoteTierConsumerAgent} from
 * repeatedly attempting to read remote files and reduce CPU consumption.
 *
 * <p>It will be invoked by {@link RemoteTierConsumerAgent} to watch the required segments and scan
 * the existence status of the segments. If the segment file is found, it will notify the
 * availability of segment file.
 */
public class RemoteStorageScanner implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteStorageScanner.class);

    /** The max retry time of checking file status through remote filesystem. */
    private static final int MAX_RETRY_TIME = 100;

    /** The initial scan interval is 100ms. */
    private static final int INITIAL_SCAN_INTERVAL_MS = 100;

    /** The max scan interval is 10000ms. */
    private static final int MAX_SCAN_INTERVAL_MS = 10_000;

    /** Executor to scan the existence status of segment files on remote storage. */
    private final ScheduledExecutorService scannerExecutor =
            Executors.newScheduledThreadPool(
                    1, new ExecutorThreadFactory("remote storage scanner"));

    /** The key is partition id and subpartition id, the value is required segment id. */
    private final Map<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>, Integer>
            requiredSegmentIds;

    /**
     * The key is partition id and subpartition id, the value is max id of written segment files in
     * the subpartition.
     */
    private final Map<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>, Integer>
            scannedMaxSegmentIds;

    private final String baseRemoteStoragePath;

    private final ScanStrategy scanStrategy;

    private final FileSystem remoteFileSystem;

    @Nullable private AvailabilityNotifier notifier;

    private int lastInterval = INITIAL_SCAN_INTERVAL_MS;

    private int currentRetryTime = 0;

    public RemoteStorageScanner(String baseRemoteStoragePath) {
        this.baseRemoteStoragePath = baseRemoteStoragePath;
        this.requiredSegmentIds = new ConcurrentHashMap<>();
        this.scannedMaxSegmentIds = new ConcurrentHashMap<>();
        this.scanStrategy = new ScanStrategy(MAX_SCAN_INTERVAL_MS);
        this.remoteFileSystem = createFileSystem();
    }

    private FileSystem createFileSystem() {
        FileSystem fileSystem = null;
        try {
            fileSystem = new Path(baseRemoteStoragePath).getFileSystem();
        } catch (IOException e) {
            ExceptionUtils.rethrow(
                    e, "Failed to initialize file system on the path: " + baseRemoteStoragePath);
        }
        return fileSystem;
    }

    /** Start the executor. */
    public void start() {
        synchronized (scannerExecutor) {
            if (!scannerExecutor.isShutdown()) {
                scannerExecutor.schedule(this, lastInterval, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Watch the segment for a specific subpartition in the {@link RemoteStorageScanner}.
     *
     * <p>If a segment with a larger or equal id already exists, the current segment won't be
     * watched.
     *
     * <p>If a segment with a smaller segment id is still being watched, the current segment will
     * replace it because the smaller segment should have been consumed. This method ensures that
     * only one segment file can be watched for each subpartition.
     *
     * @param partitionId is the id of partition.
     * @param subpartitionId is the id of subpartition.
     * @param segmentId is the id of segment.
     */
    public void watchSegment(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId> key =
                Tuple2.of(partitionId, subpartitionId);
        scannedMaxSegmentIds.compute(
                key,
                (segmentKey, maxSegmentId) -> {
                    if (maxSegmentId == null || maxSegmentId < segmentId) {
                        requiredSegmentIds.put(segmentKey, segmentId);
                    }
                    return maxSegmentId;
                });
    }

    /** Close the executor. */
    public void close() {
        synchronized (scannerExecutor) {
            scannerExecutor.shutdownNow();
        }
        try {
            if (!scannerExecutor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Timeout to shutdown the flush thread.");
            }
        } catch (InterruptedException | TimeoutException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /** Iterate the watched segment ids and check related file status. */
    @Override
    public void run() {
        try {
            Iterator<
                            Map.Entry<
                                    Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>,
                                    Integer>>
                    iterator = requiredSegmentIds.entrySet().iterator();
            boolean scanned = false;
            while (iterator.hasNext()) {
                Map.Entry<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>, Integer>
                        ids = iterator.next();
                TieredStoragePartitionId partitionId = ids.getKey().f0;
                TieredStorageSubpartitionId subpartitionId = ids.getKey().f1;
                int requiredSegmentId = ids.getValue();
                int maxSegmentId = scannedMaxSegmentIds.getOrDefault(ids.getKey(), -1);
                if (maxSegmentId >= requiredSegmentId
                        && checkSegmentExist(partitionId, subpartitionId, requiredSegmentId)) {
                    scanned = true;
                    iterator.remove();
                    checkNotNull(notifier).notifyAvailable(partitionId, subpartitionId);
                } else {
                    // The segment should be watched again because it's not found.
                    // If the segment belongs to other tiers and has been consumed, the segment will
                    // be replaced by newly watched segment with larger segment id. This logic is
                    // ensured by the method {@code watchSegment}.
                    scanMaxSegmentId(partitionId, subpartitionId);
                }
            }
            lastInterval =
                    scanned ? INITIAL_SCAN_INTERVAL_MS : scanStrategy.getInterval(lastInterval);
            start();
        } catch (Throwable throwable) {
            // handle un-expected exception as unhandledExceptionHandler is not
            // worked for ScheduledExecutorService.
            FatalExitExceptionHandler.INSTANCE.uncaughtException(Thread.currentThread(), throwable);
        }
    }

    public void registerAvailabilityAndPriorityNotifier(AvailabilityNotifier retriever) {
        this.notifier = retriever;
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    /**
     * Scan the max segment id of segment files for the specific partition and subpartition. The max
     * segment id can be obtained from a file named by max segment id.
     *
     * @param partitionId the partition id.
     * @param subpartitionId the subpartition id.
     */
    private void scanMaxSegmentId(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        Path segmentFinishDir =
                getSegmentFinishDirPath(
                        baseRemoteStoragePath, partitionId, subpartitionId.getSubpartitionId());
        FileStatus[] fileStatuses = new FileStatus[0];
        try {
            if (!remoteFileSystem.exists(segmentFinishDir)) {
                return;
            }
            fileStatuses = remoteFileSystem.listStatus(segmentFinishDir);
            currentRetryTime = 0;
        } catch (Throwable t) {
            if (t instanceof java.io.FileNotFoundException) {
                return;
            }
            currentRetryTime++;
            tryThrowException(t, "Failed to list the segment finish file.");
        }
        if (fileStatuses.length != 1) {
            return;
        }
        scannedMaxSegmentIds.put(
                Tuple2.of(partitionId, subpartitionId),
                Integer.parseInt(fileStatuses[0].getPath().getName()));
    }

    private boolean checkSegmentExist(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        Path segmentPath =
                getSegmentPath(
                        baseRemoteStoragePath,
                        partitionId,
                        subpartitionId.getSubpartitionId(),
                        segmentId);
        boolean isExist = false;
        try {
            isExist = remoteFileSystem.exists(segmentPath);
            currentRetryTime = 0;
        } catch (Throwable t) {
            currentRetryTime++;
            tryThrowException(t, "Failed to check the status of segment file.");
        }
        return isExist;
    }

    private void tryThrowException(Throwable t, String logMessage) {
        LOG.warn(logMessage);
        if (currentRetryTime > MAX_RETRY_TIME) {
            throw new RuntimeException(logMessage, t);
        }
    }

    /**
     * The strategy is used to decide the scan interval of {@link RemoteStorageScanner}. The
     * interval will be updated at a double rate and restricted by max value.
     */
    static class ScanStrategy {

        private final int maxScanInterval;

        ScanStrategy(int maxScanInterval) {
            checkArgument(
                    maxScanInterval > 0,
                    "maxScanInterval must be positive, was %s",
                    maxScanInterval);
            this.maxScanInterval = maxScanInterval;
        }

        int getInterval(int lastInterval) {
            return Math.min(lastInterval * 2, maxScanInterval);
        }
    }
}
