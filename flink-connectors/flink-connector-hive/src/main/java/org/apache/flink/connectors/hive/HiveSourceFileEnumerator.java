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
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * A {@link FileEnumerator} implementation for hive source, which generates splits based on {@link
 * HiveTablePartition}s.
 */
public class HiveSourceFileEnumerator implements FileEnumerator {

    // For non-partition hive table, partitions only contains one partition which partitionValues is
    // empty.
    private final List<HiveTablePartition> partitions;
    private final JobConf jobConf;

    public HiveSourceFileEnumerator(List<HiveTablePartition> partitions, JobConf jobConf) {
        this.partitions = partitions;
        this.jobConf = jobConf;
    }

    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        return new ArrayList<>(createInputSplits(minDesiredSplits, partitions, jobConf));
    }

    public static List<HiveSourceSplit> createInputSplits(
            int minNumSplits, List<HiveTablePartition> partitions, JobConf jobConf)
            throws IOException {
        List<HiveSourceSplit> hiveSplits = new ArrayList<>();
        for (HiveTablePartition partition : partitions) {
            for (InputSplit inputSplit :
                    createMRSplits(minNumSplits, partition.getStorageDescriptor(), jobConf)) {
                Preconditions.checkState(
                        inputSplit instanceof FileSplit,
                        "Unsupported InputSplit type: " + inputSplit.getClass().getName());
                hiveSplits.add(new HiveSourceSplit((FileSplit) inputSplit, partition, null));
            }
        }

        return hiveSplits;
    }

    public static InputSplit[] createMRSplits(
            int minNumSplits, StorageDescriptor sd, JobConf jobConf) throws IOException {
        org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(sd.getLocation());
        FileSystem fs = inputPath.getFileSystem(jobConf);
        // it's possible a partition exists in metastore but the data has been removed
        if (!fs.exists(inputPath)) {
            return new InputSplit[0];
        }
        InputFormat format;
        try {
            format =
                    (InputFormat)
                            Class.forName(
                                            sd.getInputFormat(),
                                            true,
                                            Thread.currentThread().getContextClassLoader())
                                    .newInstance();
        } catch (Exception e) {
            throw new FlinkHiveException("Unable to instantiate the hadoop input format", e);
        }
        ReflectionUtils.setConf(format, jobConf);
        // need to escape comma in the location path
        jobConf.set(INPUT_DIR, StringUtils.escapeString(sd.getLocation()));
        // TODO: we should consider how to calculate the splits according to minNumSplits in the
        // future.
        return format.getSplits(jobConf, minNumSplits);
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

        private final List<HiveTablePartition> partitions;
        private final JobConfWrapper jobConfWrapper;

        public Provider(List<HiveTablePartition> partitions, JobConfWrapper jobConfWrapper) {
            this.partitions = partitions;
            this.jobConfWrapper = jobConfWrapper;
        }

        @Override
        public FileEnumerator create() {
            return new HiveSourceFileEnumerator(partitions, jobConfWrapper.conf());
        }
    }
}
