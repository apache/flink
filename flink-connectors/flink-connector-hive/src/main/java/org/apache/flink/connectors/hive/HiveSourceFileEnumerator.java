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

package org.apache.flink.connectors.hive;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link FileEnumerator} implementation for hive source, which generates splits based on {@link
 * HiveTablePartition}s.
 */
public class HiveSourceFileEnumerator implements FileEnumerator {

    private final boolean isBucketedRead;
    // For non-partition hive table, partitions only contains one partition which partitionValues is
    // empty.
    private final List<HiveTablePartition> partitions;
    private final int threadNum;
    private final JobConf jobConf;

    public HiveSourceFileEnumerator(
            boolean isBucketedRead,
            List<HiveTablePartition> partitions,
            int threadNum,
            JobConf jobConf) {
        this.isBucketedRead = isBucketedRead;
        this.partitions = partitions;
        this.threadNum = threadNum;
        this.jobConf = jobConf;
    }

    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        return new ArrayList<>(
                createInputSplits(
                        minDesiredSplits, partitions, threadNum, jobConf, isBucketedRead));
    }

    public static List<HiveSourceSplit> createInputSplits(
            int minNumSplits, List<HiveTablePartition> partitions, int threadNum, JobConf jobConf)
            throws IOException {
        return createInputSplits(minNumSplits, partitions, threadNum, jobConf, false);
    }

    private static List<HiveSourceSplit> createInputSplits(
            int minNumSplits,
            List<HiveTablePartition> partitions,
            int threadNum,
            JobConf jobConf,
            boolean isBucketedRead)
            throws IOException {
        List<HiveSourceSplit> hiveSplits = new ArrayList<>();
        try (MRSplitsGetter splitsGetter = new MRSplitsGetter(threadNum, isBucketedRead)) {
            for (HiveTablePartitionSplits partitionSplits :
                    splitsGetter.getHiveTablePartitionMRSplits(minNumSplits, partitions, jobConf)) {
                HiveTablePartition partition = partitionSplits.getHiveTablePartition();
                for (InputSplit inputSplit : partitionSplits.getInputSplits()) {
                    Preconditions.checkState(
                            inputSplit instanceof FileSplit,
                            "Unsupported InputSplit type: " + inputSplit.getClass().getName());
                    hiveSplits.add(new HiveSourceSplit((FileSplit) inputSplit, partition, null));
                }
            }
        }
        return hiveSplits;
    }

    public static int getNumFiles(List<HiveTablePartition> partitions, JobConf jobConf)
            throws IOException {
        int numFiles = 0;
        for (HiveTablePartition partition : partitions) {
            StorageDescriptor sd = partition.getStorageDescriptor();
            org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(sd.getLocation());
            FileSystem fs = inputPath.getFileSystem(jobConf);
            // it's possible a partition exists in metastore but the data has been removed
            if (!fs.exists(inputPath)) {
                continue;
            }
            numFiles += fs.listStatus(inputPath).length;
        }
        return numFiles;
    }

    /** A factory to create {@link HiveSourceFileEnumerator}. */
    public static class Provider implements FileEnumerator.Provider {

        private static final long serialVersionUID = 1L;

        private final boolean isBucketedRead;
        private final List<HiveTablePartition> partitions;
        private final int threadNum;
        private final JobConfWrapper jobConfWrapper;

        public Provider(
                boolean isBucketedRead,
                List<HiveTablePartition> partitions,
                int threadNum,
                JobConfWrapper jobConfWrapper) {
            this.isBucketedRead = isBucketedRead;
            this.partitions = partitions;
            this.threadNum = threadNum;
            this.jobConfWrapper = jobConfWrapper;
        }

        @Override
        public FileEnumerator create() {
            return new HiveSourceFileEnumerator(
                    isBucketedRead, partitions, threadNum, jobConfWrapper.conf());
        }
    }
}
