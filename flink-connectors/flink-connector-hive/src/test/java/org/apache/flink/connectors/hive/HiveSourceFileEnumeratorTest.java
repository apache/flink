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

import org.apache.flink.connectors.hive.read.HiveSourceSplit;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HiveSourceFileEnumerator} . */
public class HiveSourceFileEnumeratorTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCalculateFilesSize() throws Exception {
        String baseFilePath =
                Objects.requireNonNull(this.getClass().getResource("/orc/test.orc")).getPath();
        long fileSize = Paths.get(baseFilePath).toFile().length();
        File wareHouse = temporaryFolder.newFolder("testCalculateFilesSize");
        int partitionNum = 10;
        long openCost = 1;
        List<HiveTablePartition> hiveTablePartitions = new ArrayList<>();
        for (int i = 0; i < partitionNum; i++) {
            // create partition directory
            Path partitionPath = Paths.get(wareHouse.getPath(), "p_" + i);
            Files.createDirectory(partitionPath);
            // copy file to the partition directory
            Files.copy(Paths.get(baseFilePath), Paths.get(partitionPath.toString(), "t.orc"));
            StorageDescriptor sd = new StorageDescriptor();
            sd.setLocation(partitionPath.toString());
            hiveTablePartitions.add(new HiveTablePartition(sd, new Properties()));
        }
        // test calculation with one single thread
        JobConf jobConf = new JobConf();
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM.key(), "1");
        long totalSize =
                HiveSourceFileEnumerator.calculateFilesSizeWithOpenCost(
                        hiveTablePartitions, jobConf, openCost);
        long expectedSize = partitionNum * (fileSize + openCost);
        assertThat(totalSize).isEqualTo(expectedSize);

        // test calculation with multiple threads
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM.key(), "3");
        totalSize =
                HiveSourceFileEnumerator.calculateFilesSizeWithOpenCost(
                        hiveTablePartitions, jobConf, openCost);
        assertThat(totalSize).isEqualTo(expectedSize);
    }

    @Test
    public void testCreateInputSplits() throws Exception {
        int numSplits = 1000;
        // create a jobConf with default configuration
        JobConf jobConf = new JobConf();
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM.key(), "1");
        File wareHouse = temporaryFolder.newFolder("testCreateInputSplits");
        // init the files for the partition
        StorageDescriptor sd = new StorageDescriptor();
        // set orc format
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setSerializationLib(OrcSerde.class.getName());
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(OrcInputFormat.class.getName());
        sd.setLocation(wareHouse.toString());
        String baseFilePath =
                Objects.requireNonNull(this.getClass().getResource("/orc/test.orc")).getPath();
        Files.copy(Paths.get(baseFilePath), Paths.get(wareHouse.toString(), "t.orc"));
        // use default configuration
        List<HiveSourceSplit> hiveSourceSplits =
                HiveSourceFileEnumerator.createInputSplits(
                        numSplits,
                        Collections.singletonList(new HiveTablePartition(sd, new Properties())),
                        jobConf,
                        false);
        // the single file is a single split
        assertThat(hiveSourceSplits.size()).isEqualTo(1);

        // set split max size and verify it works
        jobConf = new JobConf();
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM.key(), "1");
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES.key(), "10");
        // the splits should be more than the number of files
        hiveSourceSplits =
                HiveSourceFileEnumerator.createInputSplits(
                        numSplits,
                        Collections.singletonList(new HiveTablePartition(sd, new Properties())),
                        jobConf,
                        false);
        // the single file should be enumerated into two splits
        assertThat(hiveSourceSplits.size()).isEqualTo(2);

        jobConf = new JobConf();
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM.key(), "1");
        // set open cost and verify it works
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_FILE_OPEN_COST.key(), "1");
        hiveSourceSplits =
                HiveSourceFileEnumerator.createInputSplits(
                        numSplits,
                        Collections.singletonList(new HiveTablePartition(sd, new Properties())),
                        jobConf,
                        false);
        // the single file should be enumerated into two splits
        assertThat(hiveSourceSplits.size()).isEqualTo(2);
    }
}
