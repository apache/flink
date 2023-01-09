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

package org.apache.flink.connectors.hive.util;

import org.apache.flink.connectors.hive.HiveTablePartition;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HivePartitionUtils}. */
public class HivePartitionUtilsTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testHiveTablePartitionSerDe() throws Exception {
        String baseFilePath =
                Objects.requireNonNull(this.getClass().getResource("/orc/test.orc")).getPath();
        File wareHouse = temporaryFolder.newFolder("testHiveTablePartitionSerDe");
        int partitionNum = 10;
        List<HiveTablePartition> expectedHiveTablePartitions = new ArrayList<>();
        for (int i = 0; i < partitionNum; i++) {
            // create partition directory
            Path partitionPath = Paths.get(wareHouse.getPath(), "p_" + i);
            Files.createDirectory(partitionPath);
            // copy file to the partition directory
            Files.copy(Paths.get(baseFilePath), Paths.get(partitionPath.toString(), "t.orc"));
            StorageDescriptor sd = new StorageDescriptor();
            sd.setLocation(partitionPath.toString());
            expectedHiveTablePartitions.add(new HiveTablePartition(sd, new Properties()));
        }

        List<byte[]> hiveTablePartitionBytes =
                HivePartitionUtils.serializeHiveTablePartition(expectedHiveTablePartitions);

        List<HiveTablePartition> actualHiveTablePartitions =
                HivePartitionUtils.deserializeHiveTablePartition(hiveTablePartitionBytes);

        assertThat(actualHiveTablePartitions).isEqualTo(expectedHiveTablePartitions);
    }
}
