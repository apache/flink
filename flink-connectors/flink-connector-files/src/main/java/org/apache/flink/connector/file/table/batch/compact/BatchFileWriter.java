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

package org.apache.flink.connector.file.table.batch.compact;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.FileSystemFactory;
import org.apache.flink.connector.file.table.OutputFormatFactory;
import org.apache.flink.connector.file.table.PartitionComputer;
import org.apache.flink.connector.file.table.PartitionTempFileManager;
import org.apache.flink.connector.file.table.PartitionWriter;
import org.apache.flink.connector.file.table.PartitionWriterFactory;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.InputFile;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableException;

import java.util.LinkedHashMap;

/**
 * An operator for writing files in batch mode. Once creating a new file to write, the writing
 * operator will emit the written file to downstream.
 */
public class BatchFileWriter<T> extends AbstractStreamOperator<CoordinatorInput>
        implements OneInputStreamOperator<T, CoordinatorInput>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final FileSystemFactory fsFactory;
    private final Path tmpPath;
    private final String[] partitionColumns;
    private final boolean dynamicGrouped;
    private final LinkedHashMap<String, String> staticPartitions;
    private final PartitionComputer<T> computer;
    private final OutputFormatFactory<T> formatFactory;
    private final OutputFileConfig outputFileConfig;

    private transient PartitionWriter<T> writer;

    public BatchFileWriter(
            FileSystemFactory fsFactory,
            Path tmpPath,
            String[] partitionColumns,
            boolean dynamicGrouped,
            LinkedHashMap<String, String> staticPartitions,
            OutputFormatFactory<T> formatFactory,
            PartitionComputer<T> computer,
            OutputFileConfig outputFileConfig) {
        this.fsFactory = fsFactory;
        this.tmpPath = tmpPath;
        this.partitionColumns = partitionColumns;
        this.dynamicGrouped = dynamicGrouped;
        this.staticPartitions = staticPartitions;
        this.formatFactory = formatFactory;
        this.computer = computer;
        this.outputFileConfig = outputFileConfig;
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void open() throws Exception {
        try {
            PartitionTempFileManager fileManager =
                    new PartitionTempFileManager(
                            fsFactory,
                            tmpPath,
                            getRuntimeContext().getIndexOfThisSubtask(),
                            getRuntimeContext().getAttemptNumber(),
                            outputFileConfig);
            Configuration config =
                    getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
            PartitionWriter.Context<T> context =
                    new PartitionWriter.Context<>(config, formatFactory);

            // when write a file, the listener emit the written files and the partition the files
            // belonged
            PartitionWriter.PartitionWriterListener writerListener =
                    (partition, file) ->
                            output.collect(new StreamRecord<>(new InputFile(partition, file)));
            writer =
                    PartitionWriterFactory.<T>get(
                                    partitionColumns.length - staticPartitions.size() > 0,
                                    dynamicGrouped,
                                    staticPartitions)
                            .create(context, fileManager, computer, writerListener);

        } catch (Exception e) {
            throw new TableException("Exception in open", e);
        }
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        try {
            writer.write(element.getValue());
        } catch (Exception e) {
            throw new TableException("Exception in writeRecord", e);
        }
    }

    @Override
    public void endInput() throws Exception {}

    @Override
    public void close() throws Exception {
        try {
            staticPartitions.clear();
            writer.close();
        } catch (Exception e) {
            throw new TableException("Exception in close", e);
        }
    }
}
