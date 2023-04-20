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
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * File system {@link OutputFormat} for batch job. It commits in {@link
 * #finalizeGlobal(FinalizationContext)}.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class FileSystemOutputFormat<T>
        implements OutputFormat<T>,
                FinalizeOnMaster,
                Serializable,
                SupportsConcurrentExecutionAttempts {

    private static final long serialVersionUID = 1L;

    private final FileSystemFactory fsFactory;
    private final TableMetaStoreFactory msFactory;
    private final boolean overwrite;
    private final boolean isToLocal;
    private final Path tmpPath;
    private final String[] partitionColumns;
    private final boolean dynamicGrouped;
    private final LinkedHashMap<String, String> staticPartitions;
    private final PartitionComputer<T> computer;
    private final OutputFormatFactory<T> formatFactory;
    private final OutputFileConfig outputFileConfig;
    private final ObjectIdentifier identifier;
    private final PartitionCommitPolicyFactory partitionCommitPolicyFactory;

    private transient PartitionWriter<T> writer;
    private transient Configuration parameters;

    private FileSystemOutputFormat(
            FileSystemFactory fsFactory,
            TableMetaStoreFactory msFactory,
            boolean overwrite,
            boolean isToLocal,
            Path tmpPath,
            String[] partitionColumns,
            boolean dynamicGrouped,
            LinkedHashMap<String, String> staticPartitions,
            OutputFormatFactory<T> formatFactory,
            PartitionComputer<T> computer,
            OutputFileConfig outputFileConfig,
            ObjectIdentifier identifier,
            PartitionCommitPolicyFactory partitionCommitPolicyFactory) {
        this.fsFactory = fsFactory;
        this.msFactory = msFactory;
        this.overwrite = overwrite;
        this.isToLocal = isToLocal;
        this.tmpPath = tmpPath;
        this.partitionColumns = partitionColumns;
        this.dynamicGrouped = dynamicGrouped;
        this.staticPartitions = staticPartitions;
        this.formatFactory = formatFactory;
        this.computer = computer;
        this.outputFileConfig = outputFileConfig;
        this.identifier = identifier;
        this.partitionCommitPolicyFactory = partitionCommitPolicyFactory;
    }

    @Override
    public void finalizeGlobal(FinalizationContext context) {
        try {
            List<PartitionCommitPolicy> policies = Collections.emptyList();
            if (partitionCommitPolicyFactory != null) {
                policies =
                        partitionCommitPolicyFactory.createPolicyChain(
                                Thread.currentThread().getContextClassLoader(),
                                () -> {
                                    try {
                                        return fsFactory.create(tmpPath.toUri());
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
            }

            FileSystemCommitter committer =
                    new FileSystemCommitter(
                            fsFactory,
                            msFactory,
                            overwrite,
                            tmpPath,
                            partitionColumns.length,
                            isToLocal,
                            identifier,
                            staticPartitions,
                            policies);
            committer.commitPartitions(
                    (subtaskIndex, attemptNumber) -> {
                        try {
                            if (context.getFinishedAttempt(subtaskIndex) == attemptNumber) {
                                return true;
                            }
                        } catch (IllegalArgumentException ignored) {
                            // maybe met a dir or file which does not belong to this job
                        }
                        return false;
                    });
        } catch (Exception e) {
            throw new TableException("Exception in finalizeGlobal", e);
        } finally {
            try {
                fsFactory.create(tmpPath.toUri()).delete(tmpPath, true);
            } catch (IOException ignore) {
            }
        }
    }

    @Override
    public void configure(Configuration parameters) {
        this.parameters = parameters;
    }

    @Override
    public void open(InitializationContext context) throws IOException {
        try {
            PartitionTempFileManager fileManager =
                    new PartitionTempFileManager(
                            fsFactory,
                            tmpPath,
                            context.getTaskNumber(),
                            context.getAttemptNumber(),
                            outputFileConfig);
            PartitionWriter.Context<T> writerContext =
                    new PartitionWriter.Context<>(parameters, formatFactory);
            writer =
                    PartitionWriterFactory.<T>get(
                                    partitionColumns.length - staticPartitions.size() > 0,
                                    dynamicGrouped,
                                    staticPartitions)
                            .create(
                                    writerContext,
                                    fileManager,
                                    computer,
                                    new PartitionWriter.DefaultPartitionWriterListener());
        } catch (Exception e) {
            throw new TableException("Exception in open", e);
        }
    }

    @Override
    public void writeRecord(T record) {
        try {
            writer.write(record);
        } catch (Exception e) {
            throw new TableException("Exception in writeRecord", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
        } catch (Exception e) {
            throw new TableException("Exception in close", e);
        }
    }

    /** Builder to build {@link FileSystemOutputFormat}. */
    public static class Builder<T> {

        private String[] partitionColumns;
        private OutputFormatFactory<T> formatFactory;
        private TableMetaStoreFactory metaStoreFactory;
        private Path tmpPath;

        private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();
        private boolean dynamicGrouped = false;
        private boolean overwrite = false;
        private boolean isToLocal = false;
        private FileSystemFactory fileSystemFactory = FileSystem::get;

        private PartitionComputer<T> computer;

        private OutputFileConfig outputFileConfig = new OutputFileConfig("", "");

        private ObjectIdentifier identifier;
        private PartitionCommitPolicyFactory partitionCommitPolicyFactory;

        public Builder<T> setPartitionColumns(String[] partitionColumns) {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder<T> setStaticPartitions(LinkedHashMap<String, String> staticPartitions) {
            this.staticPartitions = staticPartitions;
            return this;
        }

        public Builder<T> setDynamicGrouped(boolean dynamicGrouped) {
            this.dynamicGrouped = dynamicGrouped;
            return this;
        }

        public Builder<T> setFormatFactory(OutputFormatFactory<T> formatFactory) {
            this.formatFactory = formatFactory;
            return this;
        }

        public Builder<T> setFileSystemFactory(FileSystemFactory fileSystemFactory) {
            this.fileSystemFactory = fileSystemFactory;
            return this;
        }

        public Builder<T> setMetaStoreFactory(TableMetaStoreFactory metaStoreFactory) {
            this.metaStoreFactory = metaStoreFactory;
            return this;
        }

        public Builder<T> setOverwrite(boolean overwrite) {
            this.overwrite = overwrite;
            return this;
        }

        public Builder<T> setIsToLocal(boolean isToLocal) {
            this.isToLocal = isToLocal;
            return this;
        }

        public Builder<T> setTempPath(Path tmpPath) {
            this.tmpPath = tmpPath;
            return this;
        }

        public Builder<T> setPartitionComputer(PartitionComputer<T> computer) {
            this.computer = computer;
            return this;
        }

        public Builder<T> setOutputFileConfig(OutputFileConfig outputFileConfig) {
            this.outputFileConfig = outputFileConfig;
            return this;
        }

        public Builder<T> setIdentifier(ObjectIdentifier identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder<T> setPartitionCommitPolicyFactory(
                PartitionCommitPolicyFactory partitionCommitPolicyFactory) {
            this.partitionCommitPolicyFactory = partitionCommitPolicyFactory;
            return this;
        }

        public FileSystemOutputFormat<T> build() {
            checkNotNull(partitionColumns, "partitionColumns should not be null");
            checkNotNull(formatFactory, "formatFactory should not be null");
            checkNotNull(metaStoreFactory, "metaStoreFactory should not be null");
            checkNotNull(tmpPath, "tmpPath should not be null");
            checkNotNull(computer, "partitionComputer should not be null");

            return new FileSystemOutputFormat<>(
                    fileSystemFactory,
                    metaStoreFactory,
                    overwrite,
                    isToLocal,
                    tmpPath,
                    partitionColumns,
                    dynamicGrouped,
                    staticPartitions,
                    formatFactory,
                    computer,
                    outputFileConfig,
                    identifier,
                    partitionCommitPolicyFactory);
        }
    }
}
