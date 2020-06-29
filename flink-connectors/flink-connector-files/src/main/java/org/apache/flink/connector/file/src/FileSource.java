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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.SimpleSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.BlockSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.file.src.impl.ContinuousFileSplitEnumerator;
import org.apache.flink.connector.file.src.impl.FileRecordFormatAdapter;
import org.apache.flink.connector.file.src.impl.FileSourceReader;
import org.apache.flink.connector.file.src.impl.StaticFileSplitEnumerator;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A unified data source that reads files - both in batch and in streaming mode.
 *
 * <p>This source supports all (distributed) file systems and object stores that can be
 * accessed via the Flink's {@link FileSystem} class.
 *
 * <p>Start building a file source via one of the following calls:
 * <ul>
 *   <li>{@link FileSource#forRecordStreamFormat(StreamFormat, Path...)}</li>
 *   <li>{@link FileSource#forBulkFileFormat(BulkFormat, Path...)}</li>
 *   <li>{@link FileSource#forRecordFileFormat(FileRecordFormat, Path...)}</li>
 * </ul>
 * This creates a {@link FileSource.FileSourceBuilder} on which you can configure all the
 * properties of the file source.
 *
 * <h2>Batch and Streaming</h2>
 *
 * <p>This source supports both bounded/batch and continuous/streaming data inputs. For the
 * bounded/batch case, the file source processes all files under the given path(s).
 * In the continuous/streaming case, the source periodically checks the paths for new files
 * and will start reading those.
 *
 * <p>When you start creating a file source (via the {@link FileSource.FileSourceBuilder} created
 * through one of the above-mentioned methods) the source is by default in bounded/batch mode.
 * Call {@link FileSource.FileSourceBuilder#monitorContinuously(Duration)} to put the source
 * into continuous streaming mode.
 *
 * <h2>Format Types</h2>
 *
 * <p>The reading of each file happens through file readers defined by <i>file formats</i>.
 * These define the parsing logic for the contents of the file. There are multiple classes that
 * the source supports. Their interfaces trade of simplicity of implementation and flexibility/efficiency.
 * <ul>
 *     <li>A {@link StreamFormat} reads the contents of a file from a file stream. It is the simplest
 *         format to implement, and provides many features out-of-the-box (like checkpointing logic)
 *         but is limited in the optimizations it can apply (such as object reuse, batching, etc.).</li>
 *     <li>A {@link BulkFormat} reads batches of records from a file at a time. It is the most "low level"
 *         format to implement, but offers the greatest flexibility to optimize the implementation.</li>
 *     <li>A {@link FileRecordFormat} is in the middle of the trade-off spectrum between the
 *         {@code StreamFormat} and the {@code BulkFormat}.</li>
 * </ul>
 *
 * <h2>Discovering / Enumerating Files</h2>
 *
 * <p>The way that the source lists the files to be processes is defined by the {@link FileEnumerator}.
 * The {@code FileEnumerator} is responsible to select the relevant files (for example filter out
 * hidden files) and to optionally splits files into multiple regions (= file source splits) that
 * can be read in parallel).
 *
 * @param <T> The type of the events/records produced by this source.
 */
@PublicEvolving
public final class FileSource<T> implements Source<T, FileSourceSplit, PendingSplitsCheckpoint>, ResultTypeQueryable<T> {

	private static final long serialVersionUID = 1L;

	/**
	 * The default split assigner, a lazy non-locality-aware assigner.
	 */
	public static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER = SimpleSplitAssigner::new;

	/**
	 * The default file enumerator used for splittable formats.
	 * The enumerator recursively enumerates files, split files that consist of multiple distributed storage
	 * blocks into multiple splits, and filters hidden files (files starting with '.' or '_').
	 * Files with suffixes of common compression formats (for example '.gzip', '.bz2', '.xy', '.zip', ...)
	 * will not be split.
	 */
	public static final FileEnumerator.Provider DEFAULT_SPLITTABLE_FILE_ENUMERATOR = BlockSplittingRecursiveEnumerator::new;

	/**
	 * The default file enumerator used for non-splittable formats.
	 * The enumerator recursively enumerates files, creates one split for the file, and filters hidden
	 * files (files starting with '.' or '_').
	 */
	public static final FileEnumerator.Provider DEFAULT_NON_SPLITTABLE_FILE_ENUMERATOR = BlockSplittingRecursiveEnumerator::new;

	// ------------------------------------------------------------------------

	private final Path[] inputPaths;

	private final FileEnumerator.Provider enumeratorFactory;

	private final FileSplitAssigner.Provider assignerFactory;

	private final BulkFormat<T> readerFormat;

	@Nullable
	private final ContinuousEnumerationSettings continuousEnumerationSettings;

	// ------------------------------------------------------------------------

	private FileSource(
			final Path[] inputPaths,
			final FileEnumerator.Provider fileEnumerator,
			final FileSplitAssigner.Provider splitAssigner,
			final BulkFormat<T> readerFormat,
			@Nullable final ContinuousEnumerationSettings continuousEnumerationSettings) {

		checkArgument(inputPaths.length > 0);
		this.inputPaths = inputPaths;
		this.enumeratorFactory = checkNotNull(fileEnumerator);
		this.assignerFactory = checkNotNull(splitAssigner);
		this.readerFormat = checkNotNull(readerFormat);
		this.continuousEnumerationSettings = continuousEnumerationSettings;
	}

	// ------------------------------------------------------------------------
	//  Source API Methods
	// ------------------------------------------------------------------------

	@Override
	public Boundedness getBoundedness() {
		return continuousEnumerationSettings == null ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
	}

	@Override
	public SourceReader<T, FileSourceSplit> createReader(SourceReaderContext readerContext) {
		return new FileSourceReader<>(readerContext, readerFormat, readerContext.getConfiguration());
	}

	@Override
	public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> createEnumerator(
			SplitEnumeratorContext<FileSourceSplit> enumContext) {

		final FileEnumerator enumerator = enumeratorFactory.create();

		// read the initial set of splits (which is also the total set of splits for bounded sources)
		final Collection<FileSourceSplit> splits;
		try {
			// TODO - in the next cleanup pass, we should try to remove the need to "wrap unchecked" here
			splits = enumerator.enumerateSplits(inputPaths, enumContext.currentParallelism());
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not enumerate file splits", e);
		}

		return createSplitEnumerator(enumContext, enumerator, splits, null);
	}

	@Override
	public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
			SplitEnumeratorContext<FileSourceSplit> enumContext,
			PendingSplitsCheckpoint checkpoint) throws IOException {

		final FileEnumerator enumerator = enumeratorFactory.create();

		return createSplitEnumerator(enumContext, enumerator, checkpoint.getSplits(), checkpoint.getAlreadyProcessedPaths());
	}

	@Override
	public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
		return FileSourceSplitSerializer.INSTANCE;
	}

	@Override
	public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
		return PendingSplitsCheckpointSerializer.INSTANCE;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return readerFormat.getProducedType();
	}

	// ------------------------------------------------------------------------
	//  helpers
	// ------------------------------------------------------------------------

	private SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> createSplitEnumerator(
			SplitEnumeratorContext<FileSourceSplit> context,
			FileEnumerator enumerator,
			Collection<FileSourceSplit> splits,
			@Nullable Collection<Path> alreadyProcessedPaths) {

		final FileSplitAssigner splitAssigner = assignerFactory.create(splits);

		if (continuousEnumerationSettings == null) {
			// bounded case
			return new StaticFileSplitEnumerator(context, splitAssigner);
		} else {
			// unbounded case
			if (alreadyProcessedPaths == null) {
				alreadyProcessedPaths = splitsToPaths(splits);
			}

			return new ContinuousFileSplitEnumerator(
					context,
					enumerator,
					splitAssigner,
					inputPaths,
					alreadyProcessedPaths,
					continuousEnumerationSettings.getDiscoveryInterval().toMillis());
		}
	}

	private static Collection<Path> splitsToPaths(Collection<FileSourceSplit> splits) {
		return splits.stream()
			.map(FileSourceSplit::path)
			.collect(Collectors.toCollection(HashSet::new));
	}

	// ------------------------------------------------------------------------
	//  Entry-point Factory Methods
	// ------------------------------------------------------------------------

	/**
	 * Builds a new {@code FileSource} using a {@link StreamFormat} to read record-by-record from a
	 * file stream.
	 *
	 * <p>When possible, stream-based formats are generally easier (preferable) to file-based formats,
	 * because they support better default behavior around I/O batching, or better progress tracking to
	 * avoid re-doing work on recovery.
	 */
	public static <T> FileSourceBuilder<T> forRecordStreamFormat(final StreamFormat<T> reader, final Path... paths) {
		checkNotNull(reader, "reader");
		checkNotNull(paths, "paths");
		checkArgument(paths.length > 0, "paths must not be empty");

		final BulkFormat<T> bulkFormat = new StreamFormatAdapter<>(reader);
		return new FileSourceBuilder<>(paths, bulkFormat);
	}

	/**
	 * Builds a new {@code FileSource} using a {@link BulkFormat} to read batches of records
	 * from files.
	 *
	 * <p>Examples for bulk readers are compressed and vectorized formats such as ORC or Parquet.
	 */
	public static <T> FileSourceBuilder<T> forBulkFileFormat(final BulkFormat<T> reader, final Path... paths) {
		checkNotNull(reader, "reader");
		checkNotNull(paths, "paths");
		checkArgument(paths.length > 0, "paths must not be empty");

		return new FileSourceBuilder<>(paths, reader);
	}

	/**
	 * Builds a new {@code FileSource} using a {@link FileRecordFormat} to read record-by-record from a
	 * a file path.
	 *
	 * <p>A {@code FileRecordFormat} is more general than the {@link StreamFormat}, but also
	 * requires often more careful parametrization.
	 */
	public static <T> FileSourceBuilder<T> forRecordFileFormat(final FileRecordFormat<T> reader, final Path... paths) {
		checkNotNull(reader, "reader");
		checkNotNull(paths, "paths");
		checkArgument(paths.length > 0, "paths must not be empty");

		final BulkFormat<T> bulkFormat = new FileRecordFormatAdapter<>(reader);
		return new FileSourceBuilder<>(paths, bulkFormat);
	}

	// ------------------------------------------------------------------------
	//  Builder
	// ------------------------------------------------------------------------

	/**
	 * The builder for the {@code FileSource}, to configure the various behaviors.
	 *
	 * <p>Start building the source via one of the following methods:
	 * <ul>
	 *   <li>{@link FileSource#forRecordStreamFormat(StreamFormat, Path...)}</li>
	 *   <li>{@link FileSource#forBulkFileFormat(BulkFormat, Path...)}</li>
	 *   <li>{@link FileSource#forRecordFileFormat(FileRecordFormat, Path...)}</li>
	 * </ul>
	 */
	public static final class FileSourceBuilder<T> extends AbstractFileSourceBuilder<T, FileSourceBuilder<T>> {
		public FileSourceBuilder(Path[] inputPaths, BulkFormat<T> readerFormat) {
			super(inputPaths, readerFormat);
		}
	}

	/**
	 * The generic base builder. This builder carries a <i>SELF</i> type to make it convenient to
	 * extend this for subclasses, using the following pattern.
	 * <pre>{@code
	 * public class SubBuilder<T> extends AbstractFileSourceBuilder<T, SubBuilder<T>> {
	 *     ...
	 * }
	 * }</pre>
	 * That way, all return values from builder method defined here are typed to the sub-class
	 * type and support fluent chaining.
	 *
	 * <p>We don't make the publicly visible builder generic with a SELF type, because it leads to
	 * generic signatures that can look complicated and confusing.
	 */
	public static class AbstractFileSourceBuilder<T, SELF extends AbstractFileSourceBuilder<T, SELF>> {

		// mandatory - have no defaults
		private final Path[] inputPaths;
		private final BulkFormat<T> readerFormat;

		// optional - have defaults
		private FileEnumerator.Provider fileEnumerator;
		private FileSplitAssigner.Provider splitAssigner;
		@Nullable
		private ContinuousEnumerationSettings continuousSourceSettings;

		protected AbstractFileSourceBuilder(Path[] inputPaths, BulkFormat<T> readerFormat) {
			this.inputPaths = checkNotNull(inputPaths);
			this.readerFormat = checkNotNull(readerFormat);

			this.fileEnumerator = readerFormat.isSplittable()
					? DEFAULT_SPLITTABLE_FILE_ENUMERATOR
					: DEFAULT_NON_SPLITTABLE_FILE_ENUMERATOR;
			this.splitAssigner = DEFAULT_SPLIT_ASSIGNER;
		}

		/**
		 * Creates the file source with the settings applied to this builder.
		 */
		public FileSource<T> build() {
			return new FileSource<>(
				inputPaths,
				fileEnumerator,
				splitAssigner,
				readerFormat,
				continuousSourceSettings);
		}

		/**
		 * Sets this source to streaming ("continuous monitoring") mode.
		 *
		 * <p>This makes the source a "continuous streaming" source that keeps running, monitoring
		 * for new files, and reads these files when they appear and are discovered by the monitoring.
		 *
		 * <p>The interval in which the source checks for new files is the {@code discoveryInterval}.
		 * Shorter intervals mean that files are discovered more quickly, but also imply more frequent
		 * listing or directory traversal of the file system / object store.
		 */
		public SELF monitorContinuously(Duration discoveryInterval) {
			checkNotNull(discoveryInterval, "discoveryInterval");
			checkArgument(!(discoveryInterval.isNegative() || discoveryInterval.isZero()), "discoveryInterval must be > 0");

			this.continuousSourceSettings = new ContinuousEnumerationSettings(discoveryInterval);
			return self();
		}

		/**
		 * Sets this source to bounded (batch) mode.
		 *
		 * <p>In this mode, the source processes the files that are under the given paths when the
		 * application is started. Once all files are processed, the source will finish.
		 *
		 * <p>This setting is also the default behavior. This method is mainly here to "switch back" to
		 * bounded (batch) mode, or to make it explicit in the source construction.
		 */
		public SELF processStaticFileSet() {
			this.continuousSourceSettings = null;
			return self();
		}

		/**
		 * Configures the {@link FileEnumerator} for the source.
		 * The File Enumerator is responsible for selecting from the input path the set of files
		 * that should be processed (and which to filter out). Furthermore, the File Enumerator
		 * may split the files further into sub-regions, to enable parallelization beyond the number
		 * of files.
		 */
		public SELF setFileEnumerator(FileEnumerator.Provider fileEnumerator) {
			this.fileEnumerator = checkNotNull(fileEnumerator);
			return self();
		}

		/**
		 * Configures the {@link FileSplitAssigner} for the source.
		 * The File Split Assigner determines which parallel reader instance gets which
		 * {@link FileSourceSplit}, and in which order these splits are assigned.
		 */
		public SELF setSplitAssigner(FileSplitAssigner.Provider splitAssigner) {
			this.splitAssigner = checkNotNull(splitAssigner);
			return self();
		}

		@SuppressWarnings("unchecked")
		private SELF self() {
			return (SELF) this;
		}
	}
}
