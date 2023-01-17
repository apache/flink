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
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.core.fs.Path;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/**
 * {@link PartitionWriter} for grouped dynamic partition inserting. It will create a new format when
 * partition changed.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class GroupedPartitionWriter<T> implements PartitionWriter<T> {

    private final Context<T> context;
    private final PartitionTempFileManager manager;
    private final PartitionComputer<T> computer;
    private final PartitionWriterListener writerListener;

    private OutputFormat<T> currentFormat;
    private String currentPartition;

    public GroupedPartitionWriter(
            Context<T> context, PartitionTempFileManager manager, PartitionComputer<T> computer) {
        this(context, manager, computer, new DefaultPartitionWriterListener());
    }

    public GroupedPartitionWriter(
            Context<T> context,
            PartitionTempFileManager manager,
            PartitionComputer<T> computer,
            PartitionWriterListener writerListener) {
        this.context = context;
        this.manager = manager;
        this.computer = computer;
        this.writerListener = writerListener;
    }

    @Override
    public void write(T in) throws Exception {
        String partition = generatePartitionPath(computer.generatePartValues(in));
        if (!partition.equals(currentPartition)) {
            if (currentFormat != null) {
                currentFormat.close();
            }

            Path path = manager.createPartitionDir(partition);
            currentFormat = context.createNewOutputFormat(path);
            writerListener.onFileOpened(partition, path);
            currentPartition = partition;
        }
        currentFormat.writeRecord(computer.projectColumnsToWrite(in));
    }

    @Override
    public void close() throws Exception {
        if (currentFormat != null) {
            currentFormat.close();
            currentFormat = null;
        }
    }
}
