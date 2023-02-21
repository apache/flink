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

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/**
 * Dynamic partition writer to writing multiple partitions at the same time, it maybe consumes more
 * memory.
 */
@Internal
public class DynamicPartitionWriter<T> implements PartitionWriter<T> {

    private final Context<T> context;
    private final PartitionTempFileManager manager;
    private final PartitionComputer<T> computer;
    private final Map<String, OutputFormat<T>> formats;
    private final PartitionWriterListener writerListener;

    public DynamicPartitionWriter(
            Context<T> context, PartitionTempFileManager manager, PartitionComputer<T> computer) {
        this(context, manager, computer, new DefaultPartitionWriterListener());
    }

    public DynamicPartitionWriter(
            Context<T> context,
            PartitionTempFileManager manager,
            PartitionComputer<T> computer,
            PartitionWriterListener writerListener) {
        this.context = context;
        this.manager = manager;
        this.computer = computer;
        this.formats = new HashMap<>();
        this.writerListener = writerListener;
    }

    @Override
    public void write(T in) throws Exception {
        String partition = generatePartitionPath(computer.generatePartValues(in));
        OutputFormat<T> format = formats.get(partition);

        if (format == null) {
            // create a new format to write new partition.
            Path path = manager.createPartitionDir(partition);
            format = context.createNewOutputFormat(path);
            formats.put(partition, format);
            writerListener.onFileOpened(partition, path);
        }
        format.writeRecord(computer.projectColumnsToWrite(in));
    }

    @Override
    public void close() throws Exception {
        for (OutputFormat<?> format : formats.values()) {
            format.close();
        }
        formats.clear();
    }
}
