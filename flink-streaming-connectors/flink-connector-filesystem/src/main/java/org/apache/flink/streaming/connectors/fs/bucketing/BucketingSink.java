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
package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Iterator;

/**
 * Sink that emits its input elements to {@link org.apache.hadoop.fs.FileSystem} files within
 * buckets. This is integrated with the checkpointing mechanism to provide exactly once semantics.
 *
 * <p>
 * When creating the sink a {@code basePath} must be specified. The base directory contains
 * one directory for every bucket. The bucket directories themselves contain several part files.
 * These contain the actual written data.
 *
 * <p>
 * The sink uses a {@link Bucketer} to determine the bucket directories that elements should
 * be written to inside the
 * base directory. The {@code Bucketer} can, for example, use time or a property of the element
 * to determine the bucket directory. The default bucketer is a {@link DateTimeBucketer}
 * with date format string {@code ""yyyy-MM-dd--HH"}. You can specify a custom {@code Bucketer}
 * using {@link #setBucketer(Bucketer)}. For example, use
 * {@link BasePathBucketer} if you don't want to have buckets but still write part files in a
 * fault-tolerant way.
 *
 * <p>
 * The filenames of the part files contain the part prefix, the parallel subtask index of the sink
 * and a rolling counter, for example {@code "part-1-17"}. Per default the part prefix is
 * {@code "part"} but this can be configured using {@link #setPartPrefix(String)}. When a part file
 * becomes bigger than the batch size the current part file is closed, the part counter is increased and
 * a new part file is created. The batch size defaults to {@code 384MB}, this can be configured
 * using {@link #setBatchSize(long)}.
 *
 * <p>
 * In some scenarios, the buckets being written to change over time. In these cases, the sink needs to
 * determine when a bucket has become inactive, in order to flush and close the part file. A bucket is
 * regarded inactive if it hasn't been written to recently. To support this there are two configurable
 * settings: the frequency to check for inactivity, configured by
 * {@link #setInactiveBucketCheckInterval(long)} and the minimum amount of time a bucket must not be
 * written to before it should be regarded inactive, configured by
 * {@link #setInactiveBucketThreshold(long)}. Both of these parameters default to {@code 60,000 ms}.
 *
 * <p>
 * Part files can be in one of three states: in-progress, pending or finished. The reason for this
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
 * <p>
 * Note: If checkpointing is not enabled the pending files will never be moved to the finished state.
 * In that case, the pending suffix/prefix can be set to {@code ""} to make the sink work
 * in a non-fault-tolerant way but still provide output without prefixes and suffixes.
 *
 * <p>
 * The part files are written using an instance of {@link Writer}. By default
 * {@link org.apache.flink.streaming.connectors.fs.StringWriter} is used, which writes the result
 * of {@code toString()} for every element. Separated by newlines. You can configure the writer
 * using {@link #setWriter(Writer)}. For example,
 * {@link org.apache.flink.streaming.connectors.fs.SequenceFileWriter} can be used to write
 * Hadoop {@code SequenceFiles}.
 *
 * <p>
 * Example:
 *
 * <pre>{@code
 *     new BucketingSink<Tuple2<IntWritable, Text>>(outPath)
 *         .setWriter(new SequenceFileWriter<IntWritable, Text>())
 *         .setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm")
 * }</pre>
 *
 * This will create a sink that writes to {@code SequenceFiles} and rolls every minute.
 *
 * @see DateTimeBucketer
 * @see StringWriter
 * @see SequenceFileWriter
 *
 * @param <T> Type of the elements emitted by this sink
 */
public class BucketingSink<T>
		extends RichSinkFunction<T>
		implements InputTypeConfigurable, Checkpointed<BucketingSink.State<T>>, CheckpointListener, Triggerable {
	private static final long serialVersionUID = 1L;

	private static Logger LOG = LoggerFactory.getLogger(BucketingSink.class);

	// --------------------------------------------------------------------------------------------
	//  User configuration values
	// --------------------------------------------------------------------------------------------
	// These are initialized with some defaults but are meant to be changeable by the user

	/**
	 * The default maximum size of part files.
	 *
	 * 6 times the default block size
	 */
	private final long DEFAULT_BATCH_SIZE = 1024L * 1024L * 384L;

	/**
	 * The default time between checks for inactive buckets.
	 */
	private final long DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS = 60 * 1000L;

	/**
	 * The default threshold for marking a bucket as inactive and closing its part files.
	 * Buckets which haven't been written to for at least this period of time become inactive.
	 */
	private final long DEFAULT_INACTIVE_BUCKET_THRESHOLD_MS = 60 * 1000L;

	/**
	 * This is used for part files that we are writing to but which where not yet confirmed
	 * by a checkpoint.
	 */
	private final String DEFAULT_IN_PROGRESS_SUFFIX = ".in-progress";

	/**
	 * See above, but for prefix
	 */
	private final String DEFAULT_IN_PROGRESS_PREFIX = "_";

	/**
	 * This is used for part files that we are not writing to but which are not yet confirmed by
	 * checkpoint.
	 */
	private final String DEFAULT_PENDING_SUFFIX = ".pending";

	/**
	 * See above, but for prefix.
	 */
	private final String DEFAULT_PENDING_PREFIX = "_";

	/**
	 * When truncate() is not supported on the used FileSystem we instead write a
	 * file along the part file with this ending that contains the length up to which
	 * the part file is valid.
	 */
	private final String DEFAULT_VALID_SUFFIX = ".valid-length";

	/**
	 * See above, but for prefix.
	 */
	private final String DEFAULT_VALID_PREFIX = "_";

	/**
	 * The default prefix for part files.
	 */
	private final String DEFAULT_PART_REFIX = "part";

	/**
	 * The default timeout for asynchronous operations such as recoverLease and truncate. In
	 * milliseconds.
	 */
	private final long DEFAULT_ASYNC_TIMEOUT_MS = 60 * 1000;


	/**
	 * The base {@code Path} that stores all bucket directories.
	 */
	private final String basePath;

	/**
	 * The {@code Bucketer} that is used to determine the path of bucket directories.
	 */
	private Bucketer<T> bucketer;

	/**
	 * We have a template and call duplicate() for each parallel writer in open() to get the actual
	 * writer that is used for the part files.
	 */
	private Writer<T> writerTemplate;

	/**
	 * If this is true we remove any leftover in-progress/pending files when the sink is opened.
	 *
	 * <p>
	 * This should only be set to false if using the sink without checkpoints, to not remove
	 * the files already in the directory.
	 */
	private boolean cleanupOnOpen = true;

	private long batchSize = DEFAULT_BATCH_SIZE;
	private long inactiveBucketCheckInterval = DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS;
	private long inactiveBucketThreshold = DEFAULT_INACTIVE_BUCKET_THRESHOLD_MS;

	// These are the actually configured prefixes/suffixes
	private String inProgressSuffix = DEFAULT_IN_PROGRESS_SUFFIX;
	private String inProgressPrefix = DEFAULT_IN_PROGRESS_PREFIX;

	private String pendingSuffix = DEFAULT_PENDING_SUFFIX;
	private String pendingPrefix = DEFAULT_PENDING_PREFIX;

	private String validLengthSuffix = DEFAULT_VALID_SUFFIX;
	private String validLengthPrefix= DEFAULT_VALID_PREFIX;

	private String partPrefix = DEFAULT_PART_REFIX;

	/**
	 * The timeout for asynchronous operations such as recoverLease and truncate. In
	 * milliseconds.
	 */
	private long asyncTimeout = DEFAULT_ASYNC_TIMEOUT_MS;

	// --------------------------------------------------------------------------------------------
	//  Internal fields (not configurable by user)
	// -------------------------------------------§-------------------------------------------------

	/**
	 * Our subtask index, retrieved from the {@code RuntimeContext} in {@link #open}.
	 */
	private transient int subtaskIndex;

	/**
	 * We use reflection to get the .truncate() method, this is only available starting with
	 * Hadoop 2.7
	 */
	private transient Method refTruncate;

	/**
	 * The state object that is handled by flink from snapshot/restore. In there we store state for
	 * every open bucket: the current part file path, the valid length of the in-progress files and
	 * pending part files.
	 */
	private transient State<T> state;

	private transient org.apache.hadoop.conf.Configuration hadoopConf;

	private transient Clock clock;

	/**
	 * Creates a new {@code BucketingSink} that writes files to the given base directory.
	 *
	 * <p>
	 * This uses a{@link DateTimeBucketer} as bucketer and a {@link StringWriter} has writer.
	 * The maximum bucket size is set to 384 MB.
	 *
	 * @param basePath The directory to which to write the bucket files.
	 */
	public BucketingSink(String basePath) {
		this.basePath = basePath;
		this.bucketer = new DateTimeBucketer<T>();
		this.writerTemplate = new StringWriter<>();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		if (this.writerTemplate instanceof InputTypeConfigurable) {
			((InputTypeConfigurable) writerTemplate).setInputType(type, executionConfig);
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

		state = new State<T>();

		Path baseDirectory = new Path(basePath);
		hadoopConf = HadoopFileSystem.getHadoopConfiguration();
		FileSystem fs = baseDirectory.getFileSystem(hadoopConf);
		refTruncate = reflectTruncate(fs);

		long currentProcessingTime =
				((StreamingRuntimeContext) getRuntimeContext()).getCurrentProcessingTime();

		checkForInactiveBuckets(currentProcessingTime);

		((StreamingRuntimeContext) getRuntimeContext()).registerTimer(
				currentProcessingTime + inactiveBucketCheckInterval, this);

		this.clock = new Clock() {
			@Override
			public long currentTimeMillis() {
				return ((StreamingRuntimeContext) getRuntimeContext()).getCurrentProcessingTime();
			}
		};

		// delete pending/in-progress files that might be left if we fail while
		// no checkpoint has yet been done
		try {
			if (fs.exists(baseDirectory) && cleanupOnOpen) {
				RemoteIterator<LocatedFileStatus> bucketFiles = fs.listFiles(baseDirectory, true);

				while (bucketFiles.hasNext()) {
					LocatedFileStatus file = bucketFiles.next();
					if (file.getPath().toString().endsWith(pendingSuffix)) {
						// only delete files that contain our subtask index
						if (file.getPath().toString().contains(partPrefix + "-" + subtaskIndex + "-")) {
							LOG.debug("(OPEN) Deleting leftover pending file {}", file.getPath().toString());
							fs.delete(file.getPath(), true);
						}
					}
					if (file.getPath().toString().endsWith(inProgressSuffix)) {
						// only delete files that contain our subtask index
						if (file.getPath().toString().contains(partPrefix + "-" + subtaskIndex + "-")) {
							LOG.debug("(OPEN) Deleting leftover in-progress file {}", file.getPath().toString());
							fs.delete(file.getPath(), true);
						}
					}
				}
			}
		} catch (IOException e) {
			LOG.error("Error while deleting leftover pending/in-progress files: {}", e);
			throw new RuntimeException("Error while deleting leftover pending/in-progress files.", e);
		}
	}

	@Override
	public void close() throws Exception {
		for (Map.Entry<String, BucketState<T>> entry : state.bucketStates.entrySet()) {
			closeCurrentPartFile(entry.getValue());
		}
	}

	@Override
	public void invoke(T value) throws Exception {
		Path bucketPath = bucketer.getBucketPath(clock, new Path(basePath), value);

		long currentProcessingTime =
				((StreamingRuntimeContext) getRuntimeContext()).getCurrentProcessingTime();

		if (!state.hasBucketState(bucketPath)) {
			state.addBucketState(bucketPath, new BucketState<T>(currentProcessingTime));
		}

		BucketState<T> bucketState = state.getBucketState(bucketPath);

		if (shouldRoll(bucketState)) {
			openNewPartFile(bucketPath, bucketState);
		}

		bucketState.writer.write(value);
		bucketState.lastWrittenToTime = currentProcessingTime;
	}

	/**
	 * Determines whether we should change the file we are writing to within a given bucket.
	 *
	 * <p>
	 * This will roll if no file was created yet or if the file size is larger than the specified size.
	 */
	private boolean shouldRoll(BucketState<T> bucketState) throws IOException {
		boolean shouldRoll = false;
		if (!bucketState.isWriterOpen) {
			shouldRoll = true;
			LOG.debug("BucketingSink {} starting new bucket.", subtaskIndex);
		} else {
			long writePosition = bucketState.writer.getPos();
			if (writePosition > batchSize) {
				shouldRoll = true;
				LOG.debug(
					"BucketingSink {} starting new bucket because file position {} is above batch size {}.",
					subtaskIndex,
					writePosition,
					batchSize);
			}
		}
		return shouldRoll;
	}

	@Override
	public void trigger(long timestamp) throws Exception {
		long currentProcessingTime =
				((StreamingRuntimeContext) getRuntimeContext()).getCurrentProcessingTime();

		checkForInactiveBuckets(currentProcessingTime);

		((StreamingRuntimeContext) getRuntimeContext()).registerTimer(
				currentProcessingTime + inactiveBucketCheckInterval, this);
	}

	/**
	 * Checks for inactive buckets, and closes them. This enables in-progress files to be moved to
	 * the pending state and finalised on the next checkpoint.
	 */
	private void checkForInactiveBuckets(long currentProcessingTime) throws Exception {

		synchronized (state.bucketStates) {
			for (Map.Entry<String, BucketState<T>> entry : state.bucketStates.entrySet()) {
				if (entry.getValue().lastWrittenToTime < currentProcessingTime - inactiveBucketThreshold) {
					LOG.debug("BucketingSink {} closing bucket due to inactivity of over {} ms.",
						subtaskIndex, inactiveBucketThreshold);
					closeCurrentPartFile(entry.getValue());
				}
			}
		}
	}

	/**
	 * Opens a new part file.
	 *
	 * <p>
	 * This closes the old bucket file and retrieves a new bucket path from the {@code Bucketer}.
	 */
	private void openNewPartFile(Path bucketPath, BucketState<T> bucketState) throws Exception {
		closeCurrentPartFile(bucketState);

		FileSystem fs = new Path(basePath).getFileSystem(hadoopConf);

		if (!fs.exists(bucketPath)) {
			try {
				if (fs.mkdirs(bucketPath)) {
					LOG.debug("Created new bucket directory: {}", bucketPath);
				}
			} catch (IOException e) {
				throw new RuntimeException("Could not create new bucket path.", e);
			}
		}

		Path partPath = new Path(bucketPath, partPrefix + "-" + subtaskIndex + "-" + bucketState.partCounter);

		// This should work since there is only one parallel subtask that tries names with
		// our subtask id. Otherwise we would run into concurrency issues here.
		while (fs.exists(partPath) || fs.exists(new Path(partPath.getParent(), pendingPrefix + partPath.getName()).suffix(pendingSuffix))) {
			bucketState.partCounter++;
			partPath = new Path(bucketPath, partPrefix + "-" + subtaskIndex + "-" + bucketState.partCounter);
		}

		// increase, so we don't have to check for this name next time
		bucketState.partCounter++;

		LOG.debug("Next part path is {}", partPath.toString());
		bucketState.currentFile = partPath.toString();

		Path inProgressPath = new Path(partPath.getParent(), inProgressPrefix + partPath.getName()).suffix(inProgressSuffix);

		// If we don't already have a writer for this bucket, create one
		if (bucketState.writer == null) {
			bucketState.writer = writerTemplate.duplicate();
		}

		bucketState.writer.open(fs, inProgressPath);
		bucketState.isWriterOpen = true;
	}

	/**
	 * Closes the current part file.
	 *
	 * <p>
	 * This moves the current in-progress part file to a pending file and adds it to the list
	 * of pending files in our bucket state.
	 */
	private void closeCurrentPartFile(BucketState<T> bucketState) throws Exception {
		if (bucketState.isWriterOpen) {
			bucketState.writer.close();
			bucketState.isWriterOpen = false;
		}

		if (bucketState.currentFile != null) {
			Path currentPartPath = new Path(bucketState.currentFile);
			Path inProgressPath = new Path(currentPartPath.getParent(), inProgressPrefix + currentPartPath.getName()).suffix(inProgressSuffix);
			Path pendingPath = new Path(currentPartPath.getParent(), pendingPrefix + currentPartPath.getName()).suffix(pendingSuffix);
			FileSystem fs = inProgressPath.getFileSystem(hadoopConf);
			fs.rename(inProgressPath, pendingPath);
			LOG.debug("Moving in-progress bucket {} to pending file {}",
				inProgressPath,
				pendingPath);
			bucketState.pendingFiles.add(currentPartPath.toString());
			bucketState.currentFile = null;
		}
	}

	/**
	 * Gets the truncate() call using reflection.
	 *
	 * <p>
	 * Note: This code comes from Flume
	 */
	private Method reflectTruncate(FileSystem fs) {
		Method m = null;
		if(fs != null) {
			Class<?> fsClass = fs.getClass();
			try {
				m = fsClass.getMethod("truncate", Path.class, long.class);
			} catch (NoSuchMethodException ex) {
				LOG.debug("Truncate not found. Will write a file with suffix '{}' " +
					" and prefix '{}' to specify how many bytes in a bucket are valid.", validLengthSuffix, validLengthPrefix);
				return null;
			}


			// verify that truncate actually works
			FSDataOutputStream outputStream;
			Path testPath = new Path(UUID.randomUUID().toString());
			try {
				outputStream = fs.create(testPath);
				outputStream.writeUTF("hello");
				outputStream.close();
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
		synchronized (state.bucketStates) {
			Iterator<Map.Entry<String, BucketState<T>>> it = state.bucketStates.entrySet().iterator();
			while (it.hasNext()) {
				BucketState<T> bucketState = it.next().getValue();
				synchronized (bucketState.pendingFilesPerCheckpoint) {
					Set<Long> pastCheckpointIds = bucketState.pendingFilesPerCheckpoint.keySet();
					Set<Long> checkpointsToRemove = new HashSet<>();
					for (Long pastCheckpointId : pastCheckpointIds) {
						if (pastCheckpointId <= checkpointId) {
							LOG.debug("Moving pending files to final location for checkpoint {}", pastCheckpointId);
							// All the pending files are buckets that have been completed but are waiting to be renamed
							// to their final name
							for (String filename : bucketState.pendingFilesPerCheckpoint.get(pastCheckpointId)) {
								Path finalPath = new Path(filename);
								Path pendingPath = new Path(finalPath.getParent(),
									pendingPrefix + finalPath.getName()).suffix(pendingSuffix);

								FileSystem fs = pendingPath.getFileSystem(hadoopConf);
								fs.rename(pendingPath, finalPath);
								LOG.debug(
									"Moving pending file {} to final location having completed checkpoint {}.",
									pendingPath,
									pastCheckpointId);
							}
							checkpointsToRemove.add(pastCheckpointId);
						}
					}
					if (!bucketState.isWriterOpen && bucketState.pendingFiles.isEmpty()) {
						// We've dealt with all the pending files and the writer for this bucket is not currently open.
						// Therefore this bucket is currently inactive and we can remove it from our state.
						it.remove();
					} else {
						for (Long toRemove : checkpointsToRemove) {
							bucketState.pendingFilesPerCheckpoint.remove(toRemove);
						}
					}
				}
			}
		}
	}

	@Override
	public State<T> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		synchronized (state.bucketStates) {
			for (BucketState<T> bucketState : state.bucketStates.values()) {
				if (bucketState.isWriterOpen) {
					long pos = bucketState.writer.flush();
					bucketState.currentFileValidLength = pos;
				}
				synchronized (bucketState.pendingFilesPerCheckpoint) {
					bucketState.pendingFilesPerCheckpoint.put(checkpointId, bucketState.pendingFiles);
				}
				bucketState.pendingFiles = new ArrayList<>();
			}
		}
		return state;
	}

	@Override
	public void restoreState(State<T> state) {
		this.state = state;

		FileSystem fs;
		try {
			fs = new Path(basePath).getFileSystem(HadoopFileSystem.getHadoopConfiguration());
		} catch (IOException e) {
			LOG.error("Error while creating FileSystem in checkpoint restore.", e);
			throw new RuntimeException("Error while creating FileSystem in checkpoint restore.", e);
		}

		for (BucketState<T> bucketState : state.bucketStates.values()) {
			// we can clean all the pending files since they where renamed to final files
			// after this checkpoint was successful
			bucketState.pendingFiles.clear();

			if (bucketState.currentFile != null) {
				// We were writing to a file when the last checkpoint occured. This file can either
				// be still in-progress or became a pending file at some point after the checkpoint.
				// Either way, we have to truncate it back to a valid state (or write a .valid-length)
				// file that specifies up to which length it is valid and rename it to the final name
				// before starting a new bucket file.
				Path partPath = new Path(bucketState.currentFile);
				try {
					Path partPendingPath = new Path(partPath.getParent(), pendingPrefix + partPath.getName()).suffix(
						pendingSuffix);
					Path partInProgressPath = new Path(partPath.getParent(), inProgressPrefix + partPath.getName()).suffix(inProgressSuffix);

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

					refTruncate = reflectTruncate(fs);
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
						LOG.debug("Writing valid-length file for {} to specify valid length {}", partPath, bucketState.currentFileValidLength);
						Path validLengthFilePath = new Path(partPath.getParent(), validLengthPrefix + partPath.getName()).suffix(validLengthSuffix);
						if (!fs.exists(validLengthFilePath)) {
							FSDataOutputStream lengthFileOut = fs.create(validLengthFilePath);
							lengthFileOut.writeUTF(Long.toString(bucketState.currentFileValidLength));
							lengthFileOut.close();
						}
					}

					// Now that we've restored the bucket to a valid state, reset the current file info
					bucketState.currentFile = null;
					bucketState.currentFileValidLength = -1;
				} catch (IOException e) {
					LOG.error("Error while restoring BucketingSink state.", e);
					throw new RuntimeException("Error while restoring BucketingSink state.", e);
				} catch (InvocationTargetException | IllegalAccessException e) {
					LOG.error("Cound not invoke truncate.", e);
					throw new RuntimeException("Could not invoke truncate.", e);
				}
			}

			LOG.debug("Clearing pending/in-progress files.");

			// Move files that are confirmed by a checkpoint but did not get moved to final location
			// because the checkpoint notification did not happen before a failure

			Set<Long> pastCheckpointIds = bucketState.pendingFilesPerCheckpoint.keySet();
			LOG.debug("Moving pending files to final location on restore.");
			for (Long pastCheckpointId : pastCheckpointIds) {
				// All the pending files are buckets that have been completed but are waiting to be renamed
				// to their final name
				for (String filename : bucketState.pendingFilesPerCheckpoint.get(pastCheckpointId)) {
					Path finalPath = new Path(filename);
					Path pendingPath = new Path(finalPath.getParent(), pendingPrefix + finalPath.getName()).suffix(pendingSuffix);

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

		// we need to get this here since open() has not yet been called
		int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		// delete pending files
		try {

			RemoteIterator<LocatedFileStatus> bucketFiles = fs.listFiles(new Path(basePath), true);

			while (bucketFiles.hasNext()) {
				LocatedFileStatus file = bucketFiles.next();
				if (file.getPath().toString().endsWith(pendingSuffix)) {
					// only delete files that contain our subtask index
					if (file.getPath().toString().contains(partPrefix + "-" + subtaskIndex + "-")) {
						LOG.debug("(RESTORE) Deleting pending file {}", file.getPath().toString());
						fs.delete(file.getPath(), true);
					}
				}
				if (file.getPath().toString().endsWith(inProgressSuffix)) {
					// only delete files that contain our subtask index
					if (file.getPath().toString().contains(partPrefix + "-" + subtaskIndex + "-")) {
						LOG.debug("(RESTORE) Deleting in-progress file {}", file.getPath().toString());
						fs.delete(file.getPath(), true);
					}
				}
			}
		} catch (IOException e) {
			LOG.error("Error while deleting old pending files: {}", e);
			throw new RuntimeException("Error while deleting old pending files.", e);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Setters for User configuration values
	// --------------------------------------------------------------------------------------------

	/**
	 * Sets the maximum bucket size in bytes.
	 *
	 * <p>
	 * When a bucket part file becomes larger than this size a new bucket part file is started and
	 * the old one is closed. The name of the bucket files depends on the {@link Bucketer}.
	 *
	 * @param batchSize The bucket part file size in bytes.
	 */
	public BucketingSink<T> setBatchSize(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Sets the default time between checks for inactive buckets.
	 *
	 * @param interval The timeout, in milliseconds.
	 */
	public BucketingSink<T> setInactiveBucketCheckInterval(long interval) {
		this.inactiveBucketCheckInterval = interval;
		return this;
	}

	/**
	 * Sets the default threshold for marking a bucket as inactive and closing its part files.
	 * Buckets which haven't been written to for at least this period of time become inactive.
	 *
	 * @param threshold The timeout, in milliseconds.
	 */
	public BucketingSink<T> setInactiveBucketThreshold(long threshold) {
		this.inactiveBucketThreshold = threshold;
		return this;
	}

	/**
	 * Sets the {@link Bucketer} to use for determining the bucket files to write to.
	 *
	 * @param bucketer The bucketer to use.
	 */
	public BucketingSink<T> setBucketer(Bucketer<T> bucketer) {
		this.bucketer = bucketer;
		return this;
	}

	/**
	 * Sets the {@link Writer} to be used for writing the incoming elements to bucket files.
	 *
	 * @param writer The {@code Writer} to use.
	 */
	public BucketingSink<T> setWriter(Writer<T> writer) {
		this.writerTemplate = writer;
		return this;
	}

	/**
	 * Sets the suffix of in-progress part files. The default is {@code "in-progress"}.
	 */
	public BucketingSink<T> setInProgressSuffix(String inProgressSuffix) {
		this.inProgressSuffix = inProgressSuffix;
		return this;
	}

	/**
	 * Sets the prefix of in-progress part files. The default is {@code "_"}.
	 */
	public BucketingSink<T> setInProgressPrefix(String inProgressPrefix) {
		this.inProgressPrefix = inProgressPrefix;
		return this;
	}

	/**
	 * Sets the suffix of pending part files. The default is {@code ".pending"}.
	 */
	public BucketingSink<T> setPendingSuffix(String pendingSuffix) {
		this.pendingSuffix = pendingSuffix;
		return this;
	}

	/**
	 * Sets the prefix of pending part files. The default is {@code "_"}.
	 */
	public BucketingSink<T> setPendingPrefix(String pendingPrefix) {
		this.pendingPrefix = pendingPrefix;
		return this;
	}

	/**
	 * Sets the suffix of valid-length files. The default is {@code ".valid-length"}.
	 */
	public BucketingSink<T> setValidLengthSuffix(String validLengthSuffix) {
		this.validLengthSuffix = validLengthSuffix;
		return this;
	}

	/**
	 * Sets the prefix of valid-length files. The default is {@code "_"}.
	 */
	public BucketingSink<T> setValidLengthPrefix(String validLengthPrefix) {
		this.validLengthPrefix = validLengthPrefix;
		return this;
	}

	/**
	 * Sets the prefix of part files.  The default is {@code "part"}.
	 */
	public BucketingSink<T> setPartPrefix(String partPrefix) {
		this.partPrefix = partPrefix;
		return this;
	}

	/**
	 * Disable cleanup of leftover in-progress/pending files when the sink is opened.
	 *
	 * <p>
	 * This should only be disabled if using the sink without checkpoints, to not remove
	 * the files already in the directory.
	 */
	public BucketingSink<T> disableCleanupOnOpen() {
		this.cleanupOnOpen = false;
		return this;
	}

	/**
	 * Sets the default timeout for asynchronous operations such as recoverLease and truncate.
	 *
	 * @param timeout The timeout, in milliseconds.
	 */
	public BucketingSink<T> setAsyncTimeout(long timeout) {
		this.asyncTimeout = timeout;
		return this;
	}

	// --------------------------------------------------------------------------------------------
	//  Internal Classes
	// --------------------------------------------------------------------------------------------

	/**
	 * This is used during snapshot/restore to keep track of in-progress buckets.
	 * For each bucket, we maintain a state.
	 */
	static final class State<T> implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * For every bucket directory (key), we maintain a bucket state (value).
		 */
		final Map<String, BucketState<T>> bucketStates = new HashMap<>();

		boolean hasBucketState(Path bucketPath) {
			synchronized (bucketStates) {
				return bucketStates.containsKey(bucketPath.toString());
			}
		}

		void addBucketState(Path bucketPath, BucketState<T> state) {
			synchronized (bucketStates) {
				bucketStates.put(bucketPath.toString(), state);
			}
		}

		BucketState<T> getBucketState(Path bucketPath) {
			synchronized (bucketStates) {
				return bucketStates.get(bucketPath.toString());
			}
		}
	}

	/**
	 * This is used for keeping track of the current in-progress buckets and files that we mark
	 * for moving from pending to final location after we get a checkpoint-complete notification.
	 */
	static final class BucketState<T> implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * The file that was in-progress when the last checkpoint occurred.
		 */
		String currentFile = null;

		/**
		 * The valid length of the in-progress file at the time of the last checkpoint.
		 */
		long currentFileValidLength = -1;

		/**
		 * The time this bucket was last written to.
		 */
		long lastWrittenToTime;

		/**
		 * Pending files that accumulated since the last checkpoint.
		 */
		List<String> pendingFiles = new ArrayList<>();

		/**
		 * When doing a checkpoint we move the pending files since the last checkpoint to this map
		 * with the id of the checkpoint. When we get the checkpoint-complete notification we move
		 * pending files of completed checkpoints to their final location.
		 */
		final Map<Long, List<String>> pendingFilesPerCheckpoint = new HashMap<>();

		/**
		 * For counting the part files inside a bucket directory. Part files follow the pattern
		 * {@code "{part-prefix}-{subtask}-{count}"}. When creating new part files we increase the counter.
		 */
		private transient int partCounter;

		/**
		 * Tracks if the writer is currently opened or closed.
		 */
		private transient boolean isWriterOpen = false;

		/**
		 * The actual writer that we user for writing the part files.
		 */
		private transient Writer<T> writer;

		public BucketState(long lastWrittenToTime) {
			this.lastWrittenToTime = lastWrittenToTime;
		}
	}
}
