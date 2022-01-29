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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.util.concurrent.Executors.newDirectExecutorService;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/** Create MR splits by multi-thread for hive partitions. */
public class MRSplitsGetter implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(MRSplitsGetter.class);

    private final ExecutorService executorService;

    public MRSplitsGetter(int threadNum) {
        if (threadNum > 1) {
            executorService = Executors.newFixedThreadPool(threadNum);
        } else if (threadNum == 1) {
            executorService = newDirectExecutorService();
        } else {
            throw new IllegalArgumentException(
                    "The thread number to create hive partition splits cannot be less than 1");
        }
        LOG.info("Open {} threads to create hive partition splits.", threadNum);
    }

    public List<HiveTablePartitionSplits> getHiveTablePartitionMRSplits(
            int minNumSplits, List<HiveTablePartition> partitions, JobConf jobConf)
            throws IOException {
        LOG.info("Begin to create MR splits.");
        long startTime = System.currentTimeMillis();

        final List<Future<HiveTablePartitionSplits>> futures = new ArrayList<>();
        for (HiveTablePartition partition : partitions) {
            futures.add(
                    executorService.submit(
                            new MRSplitter(minNumSplits, partition, new JobConf(jobConf))));
        }

        int splitNum = 0;
        List<HiveTablePartitionSplits> hiveTablePartitionSplitsList = new ArrayList<>();
        try {
            for (Future<HiveTablePartitionSplits> future : futures) {
                HiveTablePartitionSplits hiveTablePartitionSplits = future.get();
                splitNum += hiveTablePartitionSplits.getInputSplits().length;
                hiveTablePartitionSplitsList.add(hiveTablePartitionSplits);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Fail to create input splits.", e);
        }

        LOG.info(
                "It took {} seconds to create {} MR splits for {} hive partitions.",
                (System.currentTimeMillis() - startTime) / 1000,
                splitNum,
                partitions.size());

        return hiveTablePartitionSplitsList;
    }

    private static class MRSplitter implements Callable<HiveTablePartitionSplits> {
        private final int minNumSplits;
        private final HiveTablePartition partition;
        private final JobConf jobConf;

        public MRSplitter(int minNumSplits, HiveTablePartition partition, JobConf jobConf) {
            this.minNumSplits = minNumSplits;
            this.partition = partition;
            this.jobConf = jobConf;
        }

        @Override
        public HiveTablePartitionSplits call() throws Exception {
            StorageDescriptor sd = partition.getStorageDescriptor();
            org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(sd.getLocation());
            FileSystem fs = inputPath.getFileSystem(jobConf);
            // it's possible a partition exists in metastore but the data has been removed
            if (!fs.exists(inputPath)) {
                return new HiveTablePartitionSplits(partition, jobConf, new InputSplit[0]);
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
            return new HiveTablePartitionSplits(
                    partition, jobConf, format.getSplits(jobConf, minNumSplits));
        }
    }

    @Override
    public void close() throws IOException {
        executorService.shutdownNow();
    }
}
