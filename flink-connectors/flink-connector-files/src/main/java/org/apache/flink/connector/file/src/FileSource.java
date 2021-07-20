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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.BlockSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.impl.FileRecordFormatAdapter;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A unified data source that reads files - both in batch and in streaming mode.
 *
 * <p>This source supports all (distributed) file systems and object stores that can be accessed via
 * the Flink's {@link FileSystem} class.
 *
 * <p>Start building a file source via one of the following calls:
 *
 * <ul>
 *   <li>{@link FileSource#forRecordStreamFormat(StreamFormat, Path...)}
 *   <li>{@link FileSource#forBulkFileFormat(BulkFormat, Path...)}
 *   <li>{@link FileSource#forRecordFileFormat(FileRecordFormat, Path...)}
 * </ul>
 *
 * <p>This creates a {@link FileSource.FileSourceBuilder} on which you can configure all the
 * properties of the file source.
 *
 * <h2>Batch and Streaming</h2>
 *
 * <p>This source supports both bounded/batch and continuous/streaming data inputs. For the
 * bounded/batch case, the file source processes all files under the given path(s). In the
 * continuous/streaming case, the source periodically checks the paths for new files and will start
 * reading those.
 *
 * <p>When you start creating a file source (via the {@link FileSource.FileSourceBuilder} created
 * through one of the above-mentioned methods) the source is by default in bounded/batch mode. Call
 * {@link FileSource.FileSourceBuilder#monitorContinuously(Duration)} to put the source into
 * continuous streaming mode.
 *
 * <h2>Format Types</h2>
 *
 * <p>The reading of each file happens through file readers defined by <i>file formats</i>. These
 * define the parsing logic for the contents of the file. There are multiple classes that the source
 * supports. Their interfaces trade of simplicity of implementation and flexibility/efficiency.
 *
 * <ul>
 *   <li>A {@link StreamFormat} reads the contents of a file from a file stream. It is the simplest
 *       format to implement, and provides many features out-of-the-box (like checkpointing logic)
 *       but is limited in the optimizations it can apply (such as object reuse, batching, etc.).
 *   <li>A {@link BulkFormat} reads batches of records from a file at a time. It is the most "low
 *       level" format to implement, but offers the greatest flexibility to optimize the
 *       implementation.
 *   <li>A {@link FileRecordFormat} is in the middle of the trade-off spectrum between the {@code
 *       StreamFormat} and the {@code BulkFormat}.
 * </ul>
 *
 * <h2>Discovering / Enumerating Files</h2>
 *
 * <p>The way that the source lists the files to be processes is defined by the {@link
 * FileEnumerator}. The {@code FileEnumerator} is responsible to select the relevant files (for
 * example filter out hidden files) and to optionally splits files into multiple regions (= file
 * source splits) that can be read in parallel).
 *
 * @param <T> The type of the events/records produced by this source.
 */
@PublicEvolving
public final class FileSource<T> extends AbstractFileSource<T, FileSourceSplit> {

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

    // ------------------------------------------------------------------------

    private FileSource(
            final Path[] inputPaths,
            final FileEnumerator.Provider fileEnumerator,
            final FileSplitAssigner.Provider splitAssigner,
            final BulkFormat<T, FileSourceSplit> readerFormat,
            @Nullable final ContinuousEnumerationSettings continuousEnumerationSettings) {

        super(
                inputPaths,
                fileEnumerator,
                splitAssigner,
                readerFormat,
                continuousEnumerationSettings);
    }

    @Override
    public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
        return FileSourceSplitSerializer.INSTANCE;
    }

    // ------------------------------------------------------------------------
    //  Entry-point Factory Methods
    // ------------------------------------------------------------------------

    /**
     * Builds a new {@code FileSource} using a {@link StreamFormat} to read record-by-record from a
     * file stream.
     *
     * <p>When possible, stream-based formats are generally easier (preferable) to file-based
     * formats, because they support better default behavior around I/O batching or progress
     * tracking (checkpoints).
     *
     * <p>Stream formats also automatically de-compress files based on the file extension. This
     * supports files ending in ".deflate" (Deflate), ".xz" (XZ), ".bz2" (BZip2), ".gz", ".gzip"
     * (GZip).
     */
    public static <T> FileSourceBuilder<T> forRecordStreamFormat(
            final StreamFormat<T> reader, final Path... paths) {
        checkNotNull(reader, "reader");
        checkNotNull(paths, "paths");
        checkArgument(paths.length > 0, "paths must not be empty");

        final BulkFormat<T, FileSourceSplit> bulkFormat = new StreamFormatAdapter<>(reader);
        return new FileSourceBuilder<>(paths, bulkFormat);
    }

    /**
     * Builds a new {@code FileSource} using a {@link BulkFormat} to read batches of records from
     * files.
     *
     * <p>Examples for bulk readers are compressed and vectorized formats such as ORC or Parquet.
     */
    public static <T> FileSourceBuilder<T> forBulkFileFormat(
            final BulkFormat<T, FileSourceSplit> reader, final Path... paths) {
        checkNotNull(reader, "reader");
        checkNotNull(paths, "paths");
        checkArgument(paths.length > 0, "paths must not be empty");

        return new FileSourceBuilder<>(paths, reader);
    }

    /**
     * Builds a new {@code FileSource} using a {@link FileRecordFormat} to read record-by-record
     * from a a file path.
     *
     * <p>A {@code FileRecordFormat} is more general than the {@link StreamFormat}, but also
     * requires often more careful parametrization.
     */
    public static <T> FileSourceBuilder<T> forRecordFileFormat(
            final FileRecordFormat<T> reader, final Path... paths) {
        checkNotNull(reader, "reader");
        checkNotNull(paths, "paths");
        checkArgument(paths.length > 0, "paths must not be empty");

        final BulkFormat<T, FileSourceSplit> bulkFormat = new FileRecordFormatAdapter<>(reader);
        return new FileSourceBuilder<>(paths, bulkFormat);
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
     *   <li>{@link FileSource#forRecordStreamFormat(StreamFormat, Path...)}
     *   <li>{@link FileSource#forBulkFileFormat(BulkFormat, Path...)}
     *   <li>{@link FileSource#forRecordFileFormat(FileRecordFormat, Path...)}
     * </ul>
     */
    public static final class FileSourceBuilder<T>
            extends AbstractFileSourceBuilder<T, FileSourceSplit, FileSourceBuilder<T>> {

        FileSourceBuilder(Path[] inputPaths, BulkFormat<T, FileSourceSplit> readerFormat) {
            super(
                    inputPaths,
                    readerFormat,
                    readerFormat.isSplittable()
                            ? DEFAULT_SPLITTABLE_FILE_ENUMERATOR
                            : DEFAULT_NON_SPLITTABLE_FILE_ENUMERATOR,
                    DEFAULT_SPLIT_ASSIGNER);
        }

        @Override
        public FileSource<T> build() {
            return new FileSource<>(
                    inputPaths,
                    fileEnumerator,
                    splitAssigner,
                    readerFormat,
                    continuousSourceSettings);
        }
    }
}
