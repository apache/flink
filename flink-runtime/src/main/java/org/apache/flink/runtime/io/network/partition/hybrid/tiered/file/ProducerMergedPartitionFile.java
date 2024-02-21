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

import java.nio.file.Path;

/**
 * The partition file in the producer-merge mode. In this mode, the shuffle data is written in the
 * producer side, the consumer side need to read multiple producers to get its partition data.
 */
public class ProducerMergedPartitionFile {

    public static final String DATA_FILE_SUFFIX = ".tier-storage.data";

    public static final String INDEX_FILE_SUFFIX = ".tier-storage.index";

    public static ProducerMergedPartitionFileWriter createPartitionFileWriter(
            Path dataFilePath, ProducerMergedPartitionFileIndex partitionFileIndex) {
        return new ProducerMergedPartitionFileWriter(dataFilePath, partitionFileIndex);
    }

    public static ProducerMergedPartitionFileReader createPartitionFileReader(
            Path dataFilePath, ProducerMergedPartitionFileIndex partitionFileIndex) {
        return new ProducerMergedPartitionFileReader(dataFilePath, partitionFileIndex);
    }
}
