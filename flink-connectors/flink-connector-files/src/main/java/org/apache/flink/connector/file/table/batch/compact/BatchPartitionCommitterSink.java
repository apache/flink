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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.file.table.FileSystemCommitter;
import org.apache.flink.connector.file.table.FileSystemFactory;
import org.apache.flink.connector.file.table.PartitionCommitPolicy;
import org.apache.flink.connector.file.table.PartitionCommitPolicyFactory;
import org.apache.flink.connector.file.table.TableMetaStoreFactory;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactOutput;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Committer operator for partition in batch mode. This is the single (non-parallel) task. It
 * collects all the partition information including partition and written files send from upstream.
 */
public class BatchPartitionCommitterSink extends RichSinkFunction<CompactOutput> {

    private static final long serialVersionUID = 1L;

    private final FileSystemFactory fsFactory;
    private final TableMetaStoreFactory msFactory;
    private final PartitionCommitPolicyFactory partitionCommitPolicyFactory;
    private final Path tmpPath;
    private final boolean overwrite;
    private final boolean isToLocal;
    private final String[] partitionColumns;
    private final LinkedHashMap<String, String> staticPartitions;
    private final ObjectIdentifier identifier;

    private transient Map<String, List<Path>> partitionsFiles;

    public BatchPartitionCommitterSink(
            FileSystemFactory fsFactory,
            TableMetaStoreFactory msFactory,
            boolean overwrite,
            boolean isToLocal,
            Path tmpPath,
            String[] partitionColumns,
            LinkedHashMap<String, String> staticPartitions,
            ObjectIdentifier identifier,
            PartitionCommitPolicyFactory partitionCommitPolicyFactory) {
        this.fsFactory = fsFactory;
        this.msFactory = msFactory;
        this.partitionCommitPolicyFactory = partitionCommitPolicyFactory;
        this.tmpPath = tmpPath;
        this.identifier = identifier;
        this.overwrite = overwrite;
        this.isToLocal = isToLocal;
        this.partitionColumns = partitionColumns;
        this.staticPartitions = staticPartitions;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        partitionsFiles = new HashMap<>();
    }

    @Override
    public void invoke(CompactMessages.CompactOutput compactOutput, Context context)
            throws Exception {
        for (Map.Entry<String, List<Path>> compactFiles :
                compactOutput.getCompactedFiles().entrySet()) {
            // collect the written partition and written files
            partitionsFiles
                    .computeIfAbsent(compactFiles.getKey(), k -> new ArrayList<>())
                    .addAll(compactFiles.getValue());
        }
    }

    @Override
    public void finish() throws Exception {
        try {
            List<PartitionCommitPolicy> policies = Collections.emptyList();
            if (partitionCommitPolicyFactory != null) {
                policies =
                        partitionCommitPolicyFactory.createPolicyChain(
                                getRuntimeContext().getUserCodeClassLoader(),
                                () -> {
                                    try {
                                        return fsFactory.create(tmpPath.toUri());
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
            }
            // commit the partitions with the given files
            // it will move the written temporary files to partition's location
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
            committer.commitPartitionsWithFiles(partitionsFiles);
        } catch (Exception e) {
            throw new TableException("Exception in finish", e);
        } finally {
            try {
                fsFactory.create(tmpPath.toUri()).delete(tmpPath, true);
            } catch (IOException ignore) {
            }
        }
    }

    @Override
    public void close() throws Exception {
        staticPartitions.clear();
        partitionsFiles.clear();
    }
}
