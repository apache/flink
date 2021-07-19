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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.filesystem.PartitionTimeExtractor;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_CLASS;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE;
import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionValues;

/**
 * Partition commit predicate by partition time and watermark, if 'watermark' > 'partition-time' +
 * 'delay', the partition is committable.
 */
public class PartitionTimeCommitPredicate implements PartitionCommitPredicate {

    private final PartitionTimeExtractor extractor;
    private final long commitDelay;
    private final List<String> partitionKeys;
    /** The time zone used to parse the long watermark value to TIMESTAMP. */
    private final ZoneId watermarkTimeZone;

    public PartitionTimeCommitPredicate(
            Configuration conf, ClassLoader cl, List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        this.commitDelay = conf.get(SINK_PARTITION_COMMIT_DELAY).toMillis();
        this.extractor =
                PartitionTimeExtractor.create(
                        cl,
                        conf.get(PARTITION_TIME_EXTRACTOR_KIND),
                        conf.get(PARTITION_TIME_EXTRACTOR_CLASS),
                        conf.get(PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN));
        this.watermarkTimeZone =
                ZoneId.of(conf.getString(SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE));
    }

    @Override
    public boolean isPartitionCommittable(PredicateContext predicateContext) {
        LocalDateTime partitionTime =
                extractor.extract(
                        partitionKeys,
                        extractPartitionValues(new Path(predicateContext.partition())));
        return watermarkHasPassedWithDelay(
                predicateContext.currentWatermark(), partitionTime, commitDelay);
    }

    /**
     * Returns the watermark has passed the partition time or not, if true means it's time to commit
     * the partition.
     */
    private boolean watermarkHasPassedWithDelay(
            long watermark, LocalDateTime partitionTime, long commitDelay) {
        // here we don't parse the long watermark to TIMESTAMP and then comparison,
        // but parse the partition timestamp to epoch mills to avoid Daylight Saving Time issue
        long epochPartTime = partitionTime.atZone(watermarkTimeZone).toInstant().toEpochMilli();
        return watermark > epochPartTime + commitDelay;
    }
}
