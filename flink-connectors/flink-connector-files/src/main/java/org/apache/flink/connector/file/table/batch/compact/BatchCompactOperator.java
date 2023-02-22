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
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
import org.apache.flink.connector.file.table.stream.compact.CompactWriter;
import org.apache.flink.connector.file.table.utils.CompactFileUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;
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
 *
 * <p>Note: if the size of the files to be compacted is 1, this operator won't do anything and just
 * emit the file to downstream. Also, the name of the files to be compacted is not a hidden file,
 * it's expected these files are in hidden or temporary directory. Please make sure it. This
 * assumption can help skip rename hidden file.
 */
public class BatchCompactOperator<T> extends AbstractStreamOperator<CompactOutput>
        implements OneInputStreamOperator<CoordinatorOutput, CompactOutput>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    public static final String UNCOMPACTED_PREFIX = "uncompacted-";
    public static final String COMPACTED_PREFIX = "compacted-";
    public static final String ATTEMPT_PREFIX = "attempt-";

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
    public void open() throws Exception {
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
            Path path = null;
            if (paths.size() == 1) {
                // for single file, we only need to move it to corresponding partition instead of
                // compacting. we make the downstream commit operator to do the moving
                path = paths.get(0);
            } else if (paths.size() > 1) {
                Path targetPath =
                        createCompactedFile(paths, getRuntimeContext().getAttemptNumber());
                path =
                        CompactFileUtils.doCompact(
                                fileSystem,
                                partition,
                                paths,
                                targetPath,
                                config,
                                readerFactory,
                                writerFactory);
            }
            if (path != null) {
                compactedFiles.computeIfAbsent(partition, k -> new ArrayList<>()).add(path);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported input message: " + value);
        }
    }

    @Override
    public void endInput() throws Exception {
        // emit the compacted files to downstream
        output.collect(new StreamRecord<>(new CompactOutput(compactedFiles)));
    }

    @Override
    public void close() throws Exception {
        compactedFiles.clear();
    }

    private static Path createCompactedFile(List<Path> uncompactedFiles, int attemptNumber) {
        Path path = convertFromUncompacted(uncompactedFiles.get(0));
        // different attempt will have different target paths to avoid different attempts will
        // write same path
        return new Path(
                path.getParent(), convertToCompactWithAttempt(attemptNumber, path.getName()));
    }

    public static Path convertFromUncompacted(Path path) {
        Preconditions.checkArgument(
                path.getName().startsWith(UNCOMPACTED_PREFIX),
                "This should be uncompacted file: " + path);
        return new Path(path.getParent(), path.getName().substring(UNCOMPACTED_PREFIX.length()));
    }

    private static String convertToCompactWithAttempt(int attemptNumber, String fileName) {
        return String.format(
                "%s%s%d-%s", COMPACTED_PREFIX, ATTEMPT_PREFIX, attemptNumber, fileName);
    }
}
