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

package org.apache.flink.connector.file.src;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.DynamicParallelismInference;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.BlockSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;

/**
 * A unified data source that reads files - both in batch and in streaming mode. This is used only
 * for test. Due to {@link FileSource} is a final class, so we can't extend it directly.
 *
 * @param <T> The type of the events/records produced by this source.
 */
@Internal
public class TestFileSource<T> extends AbstractFileSource<T, FileSourceSplit>
        implements DynamicParallelismInference {

    private static final long serialVersionUID = 1L;

    /** The default split assigner, a lazy locality-aware assigner. */
    public static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER =
            LocalityAwareSplitAssigner::new;

    /**
     * The default file enumerator used for splittable formats. The enumerator recursively
     * enumerates files, split files that consist of multiple distributed storage blocks into
     * multiple splits, and filters hidden files (files starting with '.' or '_'). Files with
     * suffixes of common compression formats (for example '.gzip', '.bz2', '.xy', '.zip', ...) will
     * not be split.
     */
    public static final FileEnumerator.Provider DEFAULT_SPLITTABLE_FILE_ENUMERATOR =
            BlockSplittingRecursiveEnumerator::new;

    /**
     * The default file enumerator used for non-splittable formats. The enumerator recursively
     * enumerates files, creates one split for the file, and filters hidden files (files starting
     * with '.' or '_').
     */
    public static final FileEnumerator.Provider DEFAULT_NON_SPLITTABLE_FILE_ENUMERATOR =
            NonSplittingRecursiveEnumerator::new;

    private final boolean isStreamingMode;
    private final ContinuousEnumerationSettings continuousEnumerationSettings;

    // ------------------------------------------------------------------------

    private TestFileSource(
            final Path[] inputPaths,
            final FileEnumerator.Provider fileEnumerator,
            final FileSplitAssigner.Provider splitAssigner,
            final BulkFormat<T, FileSourceSplit> readerFormat,
            final boolean isStreamingMode,
            @Nullable final ContinuousEnumerationSettings continuousEnumerationSettings) {

        super(
                inputPaths,
                fileEnumerator,
                splitAssigner,
                readerFormat,
                continuousEnumerationSettings);
        this.isStreamingMode = isStreamingMode;
        this.continuousEnumerationSettings = continuousEnumerationSettings;
    }

    @Override
    public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
        return FileSourceSplitSerializer.INSTANCE;
    }

    @Override
    public Boundedness getBoundedness() {
        return isStreamingMode && continuousEnumerationSettings != null
                ? Boundedness.CONTINUOUS_UNBOUNDED
                : Boundedness.BOUNDED;
    }

    @Override
    public int inferParallelism(Context dynamicParallelismContext) {
        FileEnumerator fileEnumerator = getEnumeratorFactory().create();

        Collection<FileSourceSplit> splits;
        try {
            splits =
                    fileEnumerator.enumerateSplits(
                            inputPaths,
                            dynamicParallelismContext.getParallelismInferenceUpperBound());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Could not enumerate file splits", e);
        }

        return Math.min(
                splits.size(), dynamicParallelismContext.getParallelismInferenceUpperBound());
    }

    // ------------------------------------------------------------------------
    //  Entry-point Factory Methods
    // ------------------------------------------------------------------------
    /**
     * Builds a new {@code FileSource} using a {@link BulkFormat} to read batches of records from
     * files.
     *
     * <p>Examples for bulk readers are compressed and vectorized formats such as ORC or Parquet.
     */
    public static <T> TestFileSource.TestFileSourceBuilder<T> forBulkFileFormat(
            final BulkFormat<T, FileSourceSplit> bulkFormat, final Path... paths) {
        Preconditions.checkNotNull(bulkFormat, "reader");
        Preconditions.checkNotNull(paths, "paths");
        Preconditions.checkArgument(paths.length > 0, "paths must not be empty");

        return new TestFileSource.TestFileSourceBuilder<>(paths, bulkFormat);
    }

    // ------------------------------------------------------------------------
    //  Builder
    // ------------------------------------------------------------------------

    /**
     * The builder for the {@code FileSource}, to configure the various behaviors.
     *
     * <p>Start building the source via one of the following methods:
     *
     * <ul>
     *   <li>{@link TestFileSource#forBulkFileFormat(BulkFormat, Path...)}
     * </ul>
     */
    public static final class TestFileSourceBuilder<T>
            extends AbstractFileSourceBuilder<T, FileSourceSplit, FileSource.FileSourceBuilder<T>> {

        private boolean isStreamingMode = false;

        TestFileSourceBuilder(Path[] inputPaths, BulkFormat<T, FileSourceSplit> readerFormat) {
            super(
                    inputPaths,
                    readerFormat,
                    readerFormat.isSplittable()
                            ? DEFAULT_SPLITTABLE_FILE_ENUMERATOR
                            : DEFAULT_NON_SPLITTABLE_FILE_ENUMERATOR,
                    DEFAULT_SPLIT_ASSIGNER);
        }

        public TestFileSourceBuilder<T> setStreamingMode(boolean streamingMode) {
            this.isStreamingMode = streamingMode;
            return this;
        }

        @Override
        public TestFileSource<T> build() {
            return new TestFileSource<>(
                    inputPaths,
                    fileEnumerator,
                    splitAssigner,
                    readerFormat,
                    isStreamingMode,
                    continuousSourceSettings);
        }
    }
}
