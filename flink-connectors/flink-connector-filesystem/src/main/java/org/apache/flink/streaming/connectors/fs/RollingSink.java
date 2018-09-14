/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Sink that emits its input elements to rolling {@link org.apache.hadoop.fs.FileSystem} files. This
 * is integrated with the checkpointing mechanism to provide exactly once semantics.
 *
 *
 * <p>When creating the sink a {@code basePath} must be specified. The base directory contains
 * one directory for every bucket. The bucket directories themselves contain several part files.
 * These contain the actual written data.
 *
 *
 * <p>The sink uses a {@link Bucketer} to determine the name of bucket directories inside the
 * base directory. Whenever the {@code Bucketer} returns a different directory name than
 * it returned before the sink will close the current part files inside that bucket
 * and start the new bucket directory. The default bucketer is a {@link DateTimeBucketer} with
 * date format string {@code ""yyyy-MM-dd--HH"}. You can specify a custom {@code Bucketer}
 * using {@link #setBucketer(Bucketer)}. For example, use
 * {@link NonRollingBucketer} if you don't want to have
 * buckets but still write part files in a fault-tolerant way.
 *
 *
 * <p>The filenames of the part files contain the part prefix, the parallel subtask index of the sink
 * and a rolling counter, for example {@code "part-1-17"}. Per default the part prefix is
 * {@code "part"} but this can be
 * configured using {@link #setPartPrefix(String)}. When a part file becomes bigger
 * than the batch size the current part file is closed, the part counter is increased and
 * a new part file is created. The batch size defaults to {@code 384MB}, this can be configured
 * using {@link #setBatchSize(long)}.
 *
 *
 * <p>Part files can be in one of three states: in-progress, pending or finished. The reason for this
 * is how the sink works together with the checkpointing mechanism to provide exactly-once semantics
 * and fault-tolerance. The part file that is currently being written to is in-progress. Once
 * a part file is closed for writing it becomes pending. When a checkpoint is successful the
 * currently pending files will be moved to finished. If a failure occurs the pending files
 * will be deleted to reset state to the last checkpoint. The data in in-progress files will
 * also have to be rolled back. If the {@code FileSystem} supports the {@code truncate} call
 * this will be used to reset the file back to a previous state. If not, a special file
 * with the same name as the part file and the suffix {@code ".valid-length"} will be written
 * that contains the length up to which the file contains valid data. When reading the file
 * it must be ensured that it is only read up to that point. The prefixes and suffixes for
 * the different file states and valid-length files can be configured, for example with
 * {@link #setPendingSuffix(String)}.
 *
 *
 * <p>Note: If checkpointing is not enabled the pending files will never be moved to the finished state.
 * In that case, the pending suffix/prefix can be set to {@code ""} to make the sink work
 * in a non-fault-tolerant way but still provide output without prefixes and suffixes.
 *
 *
 * <p>The part files are written using an instance of {@link Writer}. By default
 * {@link org.apache.flink.streaming.connectors.fs.StringWriter} is used, which writes the result
 * of {@code toString()} for every element. Separated by newlines. You can configure the writer
 * using {@link #setWriter(Writer)}. For example,
 * {@link org.apache.flink.streaming.connectors.fs.SequenceFileWriter} can be used to write
 * Hadoop {@code SequenceFiles}.
 *
 *
 * <p>Example:
 *
 * <pre>{@code
 *     new RollingSink<Tuple2<IntWritable, Text>>(outPath)
 *         .setWriter(new SequenceFileWriter<IntWritable, Text>())
 *         .setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm")
 * }</pre>
 *
 * <p>This will create a sink that writes to {@code SequenceFiles} and rolls every minute.
 *
 * @see DateTimeBucketer
 * @see StringWriter
 * @see SequenceFileWriter
 *
 * @param <T> Type of the elements emitted by this sink
 *
 * @deprecated use {@link BucketingSink} instead.
 */
@Deprecated
public class RollingSink<T> extends RichSinkFunction<T>
		implements InputTypeConfigurable, CheckpointedFunction,
					CheckpointListener {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RollingSink.class);

	// --------------------------------------------------------------------------------------------
	//  User configuration values
	// --------------------------------------------------------------------------------------------
	// These are initialized with some defaults but are meant to be changeable by the user

	/**
	 * The default maximum size of part files (currently {@code 384 MB}).
	 */
	private static final long DEFAULT_BATCH_SIZE = 1024L * 1024L * 384L;

	/**
	 * This is used for part files that we are writing to but which where not yet confirmed
	 * by a checkpoint.
	 */
	private static final String DEFAULT_IN_PROGRESS_SUFFIX = ".in-progress";

	/**
	 * See above, but for prefix.
	 */
	private static final String DEFAULT_IN_PROGRESS_PREFIX = "_";

	/**
	 * This is used for part files that we are not writing to but which are not yet confirmed by
	 * checkpoint.
	 */
	private static final String DEFAULT_PENDING_SUFFIX = ".pending";

	/**
	 * See above, but for prefix.
	 */
	private static final String DEFAULT_PENDING_PREFIX = "_";

	/**
	 * When truncate() is not supported on the used FileSystem we instead write a
	 * file along the part file with this ending that contains the length up to which
	 * the part file is valid.
	 */
	private static final String DEFAULT_VALID_SUFFIX = ".valid-length";

	/**
	 * See above, but for prefix.
	 */
	private static final String DEFAULT_VALID_PREFIX = "_";

	/**
	 * The default prefix for part files.
	 */
	private static final String DEFAULT_PART_REFIX = "part";

	/**
	 * The default timeout for asynchronous operations such as recoverLease and truncate. In
	 * milliseconds.
	 */
	private static final long DEFAULT_ASYNC_TIMEOUT_MS = 60 * 1000;

	/**
	 * The base {@code Path} that stores all bucket directories.
	 */
	private final String basePath;

	/**
	 * The {@code Bucketer} that is used to determine the path of bucket directories.
	 */
	private Bucketer bucketer;

	/**
	 * We have a template and call duplicate() for each parallel writer in open() to get the actual
	 * writer that is used for the part files.
	 */
	private Writer<T> writerTemplate;

	/**
	 * The actual writer that we user for writing the part files.
	 */
	private Writer<T> writer;

	/**
	 * Maximum size of part files. If files exceed this we close and create a new one in the same
	 * bucket directory.
	 */
	private long batchSize;

	// These are the actually configured prefixes/suffixes
	private String inProgressSuffix = DEFAULT_IN_PROGRESS_SUFFIX;
	private String inProgressPrefix = DEFAULT_IN_PROGRESS_PREFIX;

	private String pendingSuffix = DEFAULT_PENDING_SUFFIX;
	private String pendingPrefix = DEFAULT_PENDING_PREFIX;

	private String validLengthSuffix = DEFAULT_VALID_SUFFIX;
	private String validLengthPrefix = DEFAULT_VALID_PREFIX;

	private String partPrefix = DEFAULT_PART_REFIX;

	/**
	 * The timeout for asynchronous operations such as recoverLease and truncate. In
	 * milliseconds.
	 */
	private long asyncTimeout = DEFAULT_ASYNC_TIMEOUT_MS;

	// --------------------------------------------------------------------------------------------
	//  Internal fields (not configurable by user)
	// --------------------------------------------------------------------------------------------

	/**
	 * The part file that we are currently writing to.
	 */
	private transient Path currentPartPath;

	/**
	 * The bucket directory that we are currently filling.
	 */
	private transient Path currentBucketDirectory;

	/**
	 * For counting the part files inside a bucket directory. Part files follow the patter
	 * {@code "{part-prefix}-{subtask}-{count}"}. When creating new part files we increase the counter.
	 */
	private transient int partCounter;

	/**
	 * Tracks if the writer is currently opened or closed.
	 */
	private transient boolean isWriterOpen;

	/**
	 * We use reflection to get the .truncate() method, this is only available starting with
	 * Hadoop 2.7.
	 */
	private transient Method refTruncate;

	/**
	 * The state object that is handled by flink from snapshot/restore. In there we store the
	 * current part file path, the valid length of the in-progress files and pending part files.
	 */
	private transient BucketState bucketState;

	private transient ListState<BucketState> restoredBucketStates;

	/**
	 * User-defined FileSystem parameters.
     */
	@Nullable
	private Configuration fsConfig;

	/**
	 * The FileSystem reference.
	 */
	private transient FileSystem fs;
	/**
	 * Creates a new {@code RollingSink} that writes files to the given base directory.
	 *
	 *
	 * <p>This uses a{@link DateTimeBucketer} as bucketer and a {@link StringWriter} has writer.
	 * The maximum bucket size is set to 384 MB.
	 *
	 * @param basePath The directory to which to write the bucket files.
	 */
	public RollingSink(String basePath) {
		this.basePath = basePath;
		this.bucketer = new DateTimeBucketer();
		this.batchSize = DEFAULT_BATCH_SIZE;
		this.writerTemplate = new StringWriter<>();
	}

	/**
	 * Specify a custom {@code Configuration} that will be used when creating
	 * the {@link FileSystem} for writing.
	 */
	public RollingSink<T> setFSConfig(Configuration config) {
		this.fsConfig = new Configuration();
		fsConfig.addAll(config);
		return this;
	}

	/**
	 * Specify a custom {@code Configuration} that will be used when creating
	 * the {@link FileSystem} for writing.
	 */
	public RollingSink<T> setFSConfig(org.apache.hadoop.conf.Configuration config) {
		this.fsConfig = new Configuration();
		for (Map.Entry<String, String> entry : config) {
			fsConfig.setString(entry.getKey(), entry.getValue());
		}
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		if (this.writerTemplate instanceof InputTypeConfigurable) {
			((InputTypeConfigurable) writerTemplate).setInputType(type, executionConfig);
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		Preconditions.checkArgument(this.restoredBucketStates == null,
			"The " + getClass().getSimpleName() + " has already been initialized.");

		try {
			initFileSystem();
		} catch (IOException e) {
			LOG.error("Error while creating FileSystem when initializing the state of the RollingSink.", e);
			throw new RuntimeException("Error while creating FileSystem when initializing the state of the RollingSink.", e);
		}

		if (this.refTruncate == null) {
			this.refTruncate = reflectTruncate(fs);
		}

		OperatorStateStore stateStore = context.getOperatorStateStore();
		restoredBucketStates = stateStore.getSerializableListState("rolling-states");

		int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		if (context.isRestored()) {
			LOG.info("Restoring state for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIndex);

			for (BucketState bucketState : restoredBucketStates.get()) {
				handleRestoredBucketState(bucketState);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("{} (taskIdx= {}) restored {}", getClass().getSimpleName(), subtaskIndex, bucketState);
			}
		} else {
			LOG.info("No state to restore for the {} (taskIdx= {}).", getClass().getSimpleName(), subtaskIndex);
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		partCounter = 0;

		this.writer = writerTemplate.duplicate();

		bucketState = new BucketState();
	}

	/**
	 * Create a file system with the user-defined hdfs config.
	 * @throws IOException
	 */
	private void initFileSystem() throws IOException {
		if (fs == null) {
			Path path = new Path(basePath);
			fs = BucketingSink.createHadoopFileSystem(path, fsConfig);
		}
	}

	@Override
	public void close() throws Exception {
		closeCurrentPartFile();
	}

	@Override
	public void invoke(T value) throws Exception {
		if (shouldRoll()) {
			openNewPartFile();
		}
		writer.write(value);
	}

	/**
	 * Determines whether we should change the bucket file we are writing to.
	 *
	 *
	 * <p>This will roll if no file was created yet, if the file size is larger than the specified size
	 * or if the {@code Bucketer} determines that we should roll.
	 */
	private boolean shouldRoll() throws IOException {
		boolean shouldRoll = false;
		int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		if (!isWriterOpen) {
			shouldRoll = true;
			LOG.debug("RollingSink {} starting new initial bucket. ", subtaskIndex);
		}
		if (bucketer.shouldStartNewBucket(new Path(basePath), currentBucketDirectory)) {
			shouldRoll = true;
			LOG.debug("RollingSink {} starting new bucket because {} said we should. ", subtaskIndex, bucketer);
			// we will retrieve a new bucket base path in openNewPartFile so reset the part counter
			partCounter = 0;
		}
		if (isWriterOpen) {
			long writePosition = writer.getPos();
			if (isWriterOpen && writePosition > batchSize) {
				shouldRoll = true;
				LOG.debug(
						"RollingSink {} starting new bucket because file position {} is above batch size {}.",
						subtaskIndex,
						writePosition,
						batchSize);
			}
		}
		return shouldRoll;
	}

	/**
	 * Opens a new part file.
	 *
	 *
	 * <p>This closes the old bucket file and retrieves a new bucket path from the {@code Bucketer}.
	 */
	private void openNewPartFile() throws Exception {
		closeCurrentPartFile();

		Path newBucketDirectory = bucketer.getNextBucketPath(new Path(basePath));

		if (!newBucketDirectory.equals(currentBucketDirectory)) {
			currentBucketDirectory = newBucketDirectory;
			try {
				if (fs.mkdirs(currentBucketDirectory)) {
					LOG.debug("Created new bucket directory: {}", currentBucketDirectory);
				}
			} catch (IOException e) {
				throw new RuntimeException("Could not create base path for new rolling file.", e);
			}
		}

		int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		currentPartPath = new Path(currentBucketDirectory, partPrefix + "-" + subtaskIndex + "-" + partCounter);

		// This should work since there is only one parallel subtask that tries names with
		// our subtask id. Otherwise we would run into concurrency issues here.
		while (fs.exists(currentPartPath) ||
				fs.exists(getPendingPathFor(currentPartPath)) ||
				fs.exists(getInProgressPathFor(currentPartPath))) {
			partCounter++;
			currentPartPath = new Path(currentBucketDirectory, partPrefix + "-" + subtaskIndex + "-" + partCounter);
		}

		// increase, so we don't have to check for this name next time
		partCounter++;

		LOG.debug("Next part path is {}", currentPartPath.toString());

		Path inProgressPath = getInProgressPathFor(currentPartPath);
		writer.open(fs, inProgressPath);
		isWriterOpen = true;
	}

	private Path getPendingPathFor(Path path) {
		return new Path(path.getParent(), pendingPrefix + path.getName()).suffix(pendingSuffix);
	}

	private Path getInProgressPathFor(Path path) {
		return new Path(path.getParent(), inProgressPrefix + path.getName()).suffix(inProgressSuffix);
	}

	private Path getValidLengthPathFor(Path path) {
		return new Path(path.getParent(), validLengthPrefix + path.getName()).suffix(validLengthSuffix);
	}

	/**
	 * Closes the current part file.
	 *
	 *
	 * <p>This moves the current in-progress part file to a pending file and adds it to the list
	 * of pending files in our bucket state.
	 */
	private void closeCurrentPartFile() throws Exception {
		if (isWriterOpen) {
			writer.close();
			isWriterOpen = false;
		}

		if (currentPartPath != null) {
			Path inProgressPath = getInProgressPathFor(currentPartPath);
			Path pendingPath = getPendingPathFor(currentPartPath);
			fs.rename(inProgressPath, pendingPath);
			LOG.debug("Moving in-progress bucket {} to pending file {}", inProgressPath, pendingPath);
			this.bucketState.pendingFiles.add(currentPartPath.toString());
		}
	}

	/**
	 * Gets the truncate() call using reflection.
	 *
	 * <p><b>NOTE: </b>This code comes from Flume
	 */
	private Method reflectTruncate(FileSystem fs) {
		Method m = null;
		if (fs != null) {
			Class<?> fsClass = fs.getClass();
			try {
				m = fsClass.getMethod("truncate", Path.class, long.class);
			} catch (NoSuchMethodException ex) {
				LOG.debug("Truncate not found. Will write a file with suffix '{}' " +
						" and prefix '{}' to specify how many bytes in a bucket are valid.", validLengthSuffix, validLengthPrefix);
				return null;
			}

			// verify that truncate actually works
			Path testPath = new Path(UUID.randomUUID().toString());
			try (FSDataOutputStream outputStream = fs.create(testPath)) {
				outputStream.writeUTF("hello");
			} catch (IOException e) {
				LOG.error("Could not create file for checking if truncate works.", e);
				throw new RuntimeException("Could not create file for checking if truncate works.", e);
			}

			try {
				m.invoke(fs, testPath, 2);
			} catch (IllegalAccessException | InvocationTargetException e) {
				LOG.debug("Truncate is not supported.", e);
				m = null;
			}

			try {
				fs.delete(testPath, false);
			} catch (IOException e) {
				LOG.error("Could not delete truncate test file.", e);
				throw new RuntimeException("Could not delete truncate test file.", e);
			}
		}
		return m;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (bucketState.pendingFilesPerCheckpoint) {
			Iterator<Map.Entry<Long, List<String>>> pendingCheckpointsIt =
				bucketState.pendingFilesPerCheckpoint.entrySet().iterator();

			while (pendingCheckpointsIt.hasNext()) {
				Map.Entry<Long, List<String>> entry = pendingCheckpointsIt.next();
				Long pastCheckpointId = entry.getKey();

				if (pastCheckpointId <= checkpointId) {
					LOG.debug("Moving pending files to final location for checkpoint {}", pastCheckpointId);
					// All the pending files are buckets that have been completed but are waiting to be renamed
					// to their final name
					for (String filename : entry.getValue()) {
						Path finalPath = new Path(filename);
						Path pendingPath = getPendingPathFor(finalPath);

						fs.rename(pendingPath, finalPath);
						LOG.debug("Moving pending file {} to final location after complete checkpoint {}.",
								pendingPath, pastCheckpointId);
					}
					pendingCheckpointsIt.remove();
				}
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkNotNull(restoredBucketStates,
			"The " + getClass().getSimpleName() + " has not been properly initialized.");

		int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

		if (isWriterOpen) {
			bucketState.currentFile = currentPartPath.toString();
			bucketState.currentFileValidLength = writer.flush();
		}

		synchronized (bucketState.pendingFilesPerCheckpoint) {
			bucketState.pendingFilesPerCheckpoint.put(context.getCheckpointId(), bucketState.pendingFiles);
		}
		bucketState.pendingFiles = new ArrayList<>();

		restoredBucketStates.clear();
		restoredBucketStates.add(bucketState);

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} (taskIdx={}) checkpointed {}.", getClass().getSimpleName(), subtaskIdx, bucketState);
		}
	}

	private void handleRestoredBucketState(BucketState bucketState) {
		// we can clean all the pending files since they were renamed to
		// final files after this checkpoint was successful
		// (we re-start from the last **successful** checkpoint)
		bucketState.pendingFiles.clear();

		if (bucketState.currentFile != null) {
			// We were writing to a file when the last checkpoint occurred. This file can either
			// be still in-progress or became a pending file at some point after the checkpoint.
			// Either way, we have to truncate it back to a valid state (or write a .valid-length
			// file that specifies up to which length it is valid) and rename it to the final name
			// before starting a new bucket file.
			Path partPath = new Path(bucketState.currentFile);
			try {
				Path partPendingPath = getPendingPathFor(partPath);
				Path partInProgressPath = getInProgressPathFor(partPath);

				if (fs.exists(partPendingPath)) {
					LOG.debug("In-progress file {} has been moved to pending after checkpoint, moving to final location.", partPath);
					// has been moved to pending in the mean time, rename to final location
					fs.rename(partPendingPath, partPath);
				} else if (fs.exists(partInProgressPath)) {
					LOG.debug("In-progress file {} is still in-progress, moving to final location.", partPath);
					// it was still in progress, rename to final path
					fs.rename(partInProgressPath, partPath);
				} else if (fs.exists(partPath)) {
					LOG.debug("In-Progress file {} was already moved to final location {}.", bucketState.currentFile, partPath);
				} else {
					LOG.debug("In-Progress file {} was neither moved to pending nor is still in progress. Possibly, " +
							"it was moved to final location by a previous snapshot restore", bucketState.currentFile);
				}

				if (this.refTruncate == null) {
					this.refTruncate = reflectTruncate(fs);
				}

				// truncate it or write a ".valid-length" file to specify up to which point it is valid
				if (refTruncate != null) {
					LOG.debug("Truncating {} to valid length {}", partPath, bucketState.currentFileValidLength);
					// some-one else might still hold the lease from a previous try, we are
					// recovering, after all ...
					if (fs instanceof DistributedFileSystem) {
						DistributedFileSystem dfs = (DistributedFileSystem) fs;
						LOG.debug("Trying to recover file lease {}", partPath);
						dfs.recoverLease(partPath);
						boolean isclosed = dfs.isFileClosed(partPath);
						StopWatch sw = new StopWatch();
						sw.start();
						while (!isclosed) {
							if (sw.getTime() > asyncTimeout) {
								break;
							}
							try {
								Thread.sleep(500);
							} catch (InterruptedException e1) {
								// ignore it
							}
							isclosed = dfs.isFileClosed(partPath);
						}
					}
					Boolean truncated = (Boolean) refTruncate.invoke(fs, partPath, bucketState.currentFileValidLength);
					if (!truncated) {
						LOG.debug("Truncate did not immediately complete for {}, waiting...", partPath);

						// we must wait for the asynchronous truncate operation to complete
						StopWatch sw = new StopWatch();
						sw.start();
						long newLen = fs.getFileStatus(partPath).getLen();
						while (newLen != bucketState.currentFileValidLength) {
							if (sw.getTime() > asyncTimeout) {
								break;
							}
							try {
								Thread.sleep(500);
							} catch (InterruptedException e1) {
								// ignore it
							}
							newLen = fs.getFileStatus(partPath).getLen();
						}
						if (newLen != bucketState.currentFileValidLength) {
							throw new RuntimeException("Truncate did not truncate to right length. Should be " + bucketState.currentFileValidLength + " is " + newLen + ".");
						}
					}

				} else {
					Path validLengthFilePath = getValidLengthPathFor(partPath);
					if (!fs.exists(validLengthFilePath) && fs.exists(partPath)) {
						LOG.debug("Writing valid-length file for {} to specify valid length {}", partPath, bucketState.currentFileValidLength);
						try (FSDataOutputStream lengthFileOut = fs.create(validLengthFilePath)) {
							lengthFileOut.writeUTF(Long.toString(bucketState.currentFileValidLength));
						}
					}
				}

				// invalidate in the state object
				bucketState.currentFile = null;
				bucketState.currentFileValidLength = -1;
				isWriterOpen = false;
			} catch (IOException e) {
				LOG.error("Error while restoring RollingSink state.", e);
				throw new RuntimeException("Error while restoring RollingSink state.", e);
			} catch (InvocationTargetException | IllegalAccessException e) {
				LOG.error("Could not invoke truncate.", e);
				throw new RuntimeException("Could not invoke truncate.", e);
			}
		}

		// Move files that are confirmed by a checkpoint but did not get moved to final location
		// because the checkpoint notification did not happen before a failure

		Set<Long> pastCheckpointIds = bucketState.pendingFilesPerCheckpoint.keySet();
		LOG.debug("Moving pending files to final location on restore.");
		for (Long pastCheckpointId : pastCheckpointIds) {
			// All the pending files are buckets that have been completed but are waiting to be renamed
			// to their final name
			for (String filename : bucketState.pendingFilesPerCheckpoint.get(pastCheckpointId)) {
				Path finalPath = new Path(filename);
				Path pendingPath = getPendingPathFor(finalPath);

				try {
					if (fs.exists(pendingPath)) {
						LOG.debug("(RESTORE) Moving pending file {} to final location after complete checkpoint {}.", pendingPath, pastCheckpointId);
						fs.rename(pendingPath, finalPath);
					}
				} catch (IOException e) {
					LOG.error("(RESTORE) Error while renaming pending file {} to final path {}: {}", pendingPath, finalPath, e);
					throw new RuntimeException("Error while renaming pending file " + pendingPath + " to final path " + finalPath, e);
				}
			}
		}

		synchronized (bucketState.pendingFilesPerCheckpoint) {
			bucketState.pendingFilesPerCheckpoint.clear();
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Setters for User configuration values
	// --------------------------------------------------------------------------------------------

	/**
	 * Sets the maximum bucket size in bytes.
	 *
	 *
	 * <p>When a bucket part file becomes larger than this size a new bucket part file is started and
	 * the old one is closed. The name of the bucket files depends on the {@link Bucketer}.
	 *
	 * @param batchSize The bucket part file size in bytes.
	 */
	public RollingSink<T> setBatchSize(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Sets the {@link Bucketer} to use for determining the bucket files to write to.
	 *
	 * @param bucketer The bucketer to use.
	 */
	public RollingSink<T> setBucketer(Bucketer bucketer) {
		this.bucketer = bucketer;
		return this;
	}

	/**
	 * Sets the {@link Writer} to be used for writing the incoming elements to bucket files.
	 *
	 * @param writer The {@code Writer} to use.
	 */
	public RollingSink<T> setWriter(Writer<T> writer) {
		this.writerTemplate = writer;
		return this;
	}

	/**
	 * Sets the suffix of in-progress part files. The default is {@code "in-progress"}.
	 */
	public RollingSink<T> setInProgressSuffix(String inProgressSuffix) {
		this.inProgressSuffix = inProgressSuffix;
		return this;
	}

	/**
	 * Sets the prefix of in-progress part files. The default is {@code "_"}.
	 */
	public RollingSink<T> setInProgressPrefix(String inProgressPrefix) {
		this.inProgressPrefix = inProgressPrefix;
		return this;
	}

	/**
	 * Sets the suffix of pending part files. The default is {@code ".pending"}.
	 */
	public RollingSink<T> setPendingSuffix(String pendingSuffix) {
		this.pendingSuffix = pendingSuffix;
		return this;
	}

	/**
	 * Sets the prefix of pending part files. The default is {@code "_"}.
	 */
	public RollingSink<T> setPendingPrefix(String pendingPrefix) {
		this.pendingPrefix = pendingPrefix;
		return this;
	}

	/**
	 * Sets the suffix of valid-length files. The default is {@code ".valid-length"}.
	 */
	public RollingSink<T> setValidLengthSuffix(String validLengthSuffix) {
		this.validLengthSuffix = validLengthSuffix;
		return this;
	}

	/**
	 * Sets the prefix of valid-length files. The default is {@code "_"}.
	 */
	public RollingSink<T> setValidLengthPrefix(String validLengthPrefix) {
		this.validLengthPrefix = validLengthPrefix;
		return this;
	}

	/**
	 * Sets the prefix of part files.  The default is {@code "part"}.
	 */
	public RollingSink<T> setPartPrefix(String partPrefix) {
		this.partPrefix = partPrefix;
		return this;
	}

	/**
	 * Disable cleanup of leftover in-progress/pending files when the sink is opened.
	 *
	 *
	 * <p>This should only be disabled if using the sink without checkpoints, to not remove
	 * the files already in the directory.
	 *
	 * @deprecated This option is deprecated and remains only for backwards compatibility.
	 * We do not clean up lingering files anymore.
	 */
	@Deprecated
	public RollingSink<T> disableCleanupOnOpen() {
		return this;
	}

	/**
	 * Sets the default timeout for asynchronous operations such as recoverLease and truncate.
	 *
	 * @param timeout The timeout, in milliseconds.
	 */
	public RollingSink<T> setAsyncTimeout(long timeout) {
		this.asyncTimeout = timeout;
		return this;
	}

	// --------------------------------------------------------------------------------------------
	//  Internal Classes
	// --------------------------------------------------------------------------------------------

	/**
	 * This is used for keeping track of the current in-progress files and files that we mark
	 * for moving from pending to final location after we get a checkpoint-complete notification.
	 */
	public static final class BucketState implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * The file that was in-progress when the last checkpoint occurred.
		 */
		public String currentFile;

		/**
		 * The valid length of the in-progress file at the time of the last checkpoint.
		 */
		public long currentFileValidLength = -1;

		/**
		 * Pending files that accumulated since the last checkpoint.
		 */
		public List<String> pendingFiles = new ArrayList<>();

		/**
		 * When doing a checkpoint we move the pending files since the last checkpoint to this map
		 * with the id of the checkpoint. When we get the checkpoint-complete notification we move
		 * pending files of completed checkpoints to their final location.
		 */
		public final Map<Long, List<String>> pendingFilesPerCheckpoint = new HashMap<>();

		@Override
		public String toString() {
			return
				"In-progress=" + currentFile +
				" validLength=" + currentFileValidLength +
				" pendingForNextCheckpoint=" + pendingFiles +
				" pendingForPrevCheckpoints=" + pendingFilesPerCheckpoint;
		}
	}
}
