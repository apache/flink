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

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/**
 * {@link PartitionWriter} for grouped dynamic partition inserting. It will create a new format when
 * partition or record's bucket id changed.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class GroupedPartitionWriter<T> extends BaseBucketFileWriter<T>
        implements PartitionWriter<T> {

    private final PartitionComputer<T> partitionComputer;
    private String currentPartition;

    public GroupedPartitionWriter(
            Context<T> context,
            PartitionTempFileManager manager,
            PartitionComputer<T> partitionComputer,
            BucketIdComputer<T> bucketIdComputer) {
        super(context, manager, bucketIdComputer);
        this.partitionComputer = partitionComputer;
    }

    @Override
    public void write(T in) throws Exception {
        String partition = generatePartitionPath(partitionComputer.generatePartValues(in));
        if (!partition.equals(currentPartition)) {
            // new partition, we force update output format
            forceRenewCurrentOutputFormat(in, partition);
            currentPartition = partition;
        } else {
            mayRenewCurrentOutputFormat(in, partition);
        }
        currentOutputFormat.writeRecord(partitionComputer.projectColumnsToWrite(in));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
