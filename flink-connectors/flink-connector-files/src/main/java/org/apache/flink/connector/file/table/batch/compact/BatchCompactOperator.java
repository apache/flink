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
import org.apache.flink.connector.file.table.CompactFileUtils;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
import org.apache.flink.connector.file.table.stream.compact.CompactWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CompactOperator for compaction in batch mode. It will compact files to a target file and then
 * emit the compacted file's path to downstream operator. The main logic is similar to {@link
 * org.apache.flink.connector.file.table.stream.compact.CompactOperator} but skip some unnecessary
 * operations in batch mode.
 */
public class BatchCompactOperator<T> extends AbstractStreamOperator<CompactOutput>
        implements OneInputStreamOperator<CoordinatorOutput, CompactOutput>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final SupplierWithException<FileSystem, IOException> fsFactory;
    private final CompactReader.Factory<T> readerFactory;
    private final CompactWriter.Factory<T> writerFactory;

    private transient FileSystem fileSystem;
    private transient Map<String, List<Path>> compactedFiles;

    public BatchCompactOperator(
            SupplierWithException<FileSystem, IOException> fsFactory,
            CompactReader.Factory<T> readerFactory,
            CompactWriter.Factory<T> writerFactory) {
        this.fsFactory = fsFactory;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        fileSystem = fsFactory.get();
        compactedFiles = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<CoordinatorOutput> element) throws Exception {
        CoordinatorOutput value = element.getValue();
        if (value instanceof CompactionUnit) {
            CompactionUnit unit = (CompactionUnit) value;
            String partition = unit.getPartition();
            // these files should be merged to one file
            List<Path> paths = unit.getPaths();
            Configuration config =
                    getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
            // compact them to a single file
            Path path =
                    CompactFileUtils.doCompact(
                            getRuntimeContext().getAttemptNumber(),
                            fileSystem,
                            partition,
                            paths,
                            config,
                            this::doSingleFileMove,
                            readerFactory,
                            writerFactory);
            if (path != null) {
                compactedFiles.computeIfAbsent(partition, k -> new ArrayList<>()).add(path);
            }
        }
    }

    private boolean doSingleFileMove(Path src, Path dst) throws IOException {
        // in batch mode, we can just rename the file
        fileSystem.rename(src, dst);
        return true;
    }

    @Override
    public void endInput() throws Exception {
        // emit the compacted files to downstream
        output.collect(new StreamRecord<>(new CompactOutput(compactedFiles)));
    }
}
