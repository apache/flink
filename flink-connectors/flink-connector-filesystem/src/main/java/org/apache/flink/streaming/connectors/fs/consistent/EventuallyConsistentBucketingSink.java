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

package org.apache.flink.streaming.connectors.fs.consistent;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.action.GetPropertyAction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Sink that emits its input elements its input elements to a {@link FileSystem } with eventual consistency semantics
 * such as Amazon's S3. This is integrated with the checkpointing mechanism to provide either exactly once or at least
 * once semantics depending on the level of cooperation put forth by the consuming processes.
 *
 * <p>When creating the sink a {@code basePath} must be specified. The base directory contains one directory for every
 * checkpoint. The checkpoint directories themselves contain several part files, at least one for every parallel subtask
 * of the sink. These part files contain the actual output data.
 *
 * <p>The sink uses a {@link EventuallyConsistentBucketer} to determine which directory each file should be written to
 * inside the base directory. The {@code EventuallyConsistentBucketer} must take into account both the checkpoint id
 * as well as the checkpoint timestamp to otherwise exactly once output will not be guaranteed. The default
 * {@code EventuallyConsistentBucketer} is a {@link CheckpointWithTimestampBucketer} or you can specify a custom
 * one using {@link #setEventuallyConsistentBucketer(EventuallyConsistentBucketer)}.
 *
 * <p>The filenames of the part files contain the part prefix, the parallel subtask index of the sink
 * and a rolling counter. For example the file {@code "part-1-1"} contains the data from
 * {@code subtask 1} of the sink and is the {@code 1st} file created by that subtask. Per default
 * the part prefix is {@code "part"} but this can be configured using {@link #setPartPrefix(String)}.
 *
 * <p>An eventually consistent filesystem typically provides weak guarantee's about what operations can be performed in
 * a stable manner. In general, {@code PUT} is the only operation considered always consistent. This sink will buffer
 * data either on local disk, or another consistent file system such as {@code HDFS} and then copy all files to the
 * final destination once per checkpoint.
 *
 * <p>Because {@code DELETE} operations are not permitted, if a checkpoint fails
 * then invalid files are not removed. Instead directories containing all valid files will be marked with an empty flagFile
 * file. The default flagFile file is {@code _DONE} but you can specify a custom one using {@link #setFlagFile(String)}.
 * It is then up to consuming processes to choose their guarantees. They can either achieve exactly once semantics by
 * only consuming directories with flagFile files or at least once semantics by consuming all files.
 *
 * <p><b>NOTE:</b>
 * <ol>
 *     <li>
 *         If checkpointing is not enabled the buffered files on disk will never be moved to their final location. In
 *         the case of data be buffered on local disk this will result in data being written until all disk space is
 *         consumed.
 *     </li>
 *	    <li>
 *         The part files are written using an instance of {@link Writer}. By default, a
 *         {@link StringWriter} is used, which writes the result of {@code toString()} for
 *         every element, separated by newlines. You can configure the writer using the
 *         {@link #setWriter(Writer)}. For example, {@link org.apache.flink.streaming.connectors.fs.AvroKeyValueSinkWriter.AvroKeyValueWriter}
 *         can be used to write Hadoop {@code AvroKeyValueSinkWriter}.
 *     </li>
 * </ol>
 * @param <T>
 */
public class EventuallyConsistentBucketingSink<T>
	extends RichSinkFunction<T>
	implements InputTypeConfigurable, CheckpointedFunction, CheckpointListener {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(EventuallyConsistentBucketingSink.class);

	/**
	 * The default maximum size of part files (currently {@code 384 MB}).
	 */
	private static final long DEFAULT_BATCH_SIZE = 1024L * 1024L * 384L;

	/**
	 * The default prefix for part files.
	 */
	private static final String DEFAULT_PART_PREFIX = "part";

	/**
	 * The default flagFile file for denoting consistent buckets.
	 */
	private static final String DEFAULT_FLAG_FILE = "_DONE";

	/**
	 * The consistent file system directory where files are buffered before being copied to their final location.
	 * The default location is platform dependent tmp directory on local disk.
	 */
	private String consistentFsDir;

	/**
	 * The base {@code Path} that stores all bucket directories.
	 */
	private final String basePath;

	/**
	 * The {@code EventuallyConsistentBucketer} that is used to determine the path of bucket directories.
	 */
	private EventuallyConsistentBucketer bucketer;

	/**
	 * We have a template and call duplicate() for each parallel writer in open() to get the actual
	 * writer that is used for the part files.
	 */
	private Writer<T> writerTemplate;

	private String flagFile = DEFAULT_FLAG_FILE;

	private String partPrefix = DEFAULT_PART_PREFIX;

	private long batchSize = DEFAULT_BATCH_SIZE;

	/**
	 * User-defined FileSystem parameters.
	 */
	private Configuration fsConfig;

	/**
	 * User-defined FileSystem parameters for the buffer remoteFileSystem.
	 */
	private Configuration consistentFsConfig;

	/**
	 * The FileSystem reference.
	 */
	private transient FileSystem remoteFileSystem;

	private transient State<T> state;

	/**
	 * Store path to unwritten but required flag files
	 * in case the job fails between a checkpoint completing
	 * and the operator being notified.
	 */
	private transient ListState<String> completedBucketState;

	public EventuallyConsistentBucketingSink(String basePath) {
		this.consistentFsDir = new File(AccessController.doPrivileged(new GetPropertyAction("java.io.tmpdir"))).getPath();
		this.basePath = basePath;
		this.bucketer = new FineGrainedBucketer();
		this.writerTemplate = new StringWriter<>();
	}

	/**
	 * Specify a custom {@code Configuration} that will be used when creating
	 * the {@link FileSystem} for writing.
	 */
	public EventuallyConsistentBucketingSink<T> setFSConfig(Configuration config) {
		this.fsConfig = new Configuration();
		fsConfig.addAll(config);
		return this;
	}

	/**
	 * Sets the prefix of part files.  The default is {@code "part"}.
	 */
	public EventuallyConsistentBucketingSink<T> setPartPrefix(String partPrefix) {
		this.partPrefix = partPrefix;
		return this;
	}

	/**
	 * Sets the maximum bucket size in bytes.
	 *
	 * <p>When a bucket part file becomes larger than this size a new bucket part file is started and
	 * the old one is closed. The name of the bucket files depends on the {@link Bucketer}.
	 *
	 * @param batchSize The bucket part file size in bytes.
	 */
	public EventuallyConsistentBucketingSink<T> setBatchSize(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Specify a custom {@code Configuration} that will be used when creating
	 * the {@link FileSystem} for writing.
	 */
	public EventuallyConsistentBucketingSink<T> setFSConfig(org.apache.hadoop.conf.Configuration config) {
		this.fsConfig = new Configuration();
		for (Map.Entry<String, String> entry : config) {
			fsConfig.setString(entry.getKey(), entry.getValue());
		}
		return this;
	}

	/**
	 * Specify a custom {@code Configuration} that will be used when creating
	 * the {@link FileSystem} for writing buffer files.
	 */
	public EventuallyConsistentBucketingSink<T> setBufferFSConfig(Configuration config) {
		this.consistentFsConfig = new Configuration();
		consistentFsConfig.addAll(config);
		return this;
	}

	/**
	 * Specify a custom {@code Configuration} that will be used when creating
	 * the {@link FileSystem} for writing buffer files.
	 */
	public EventuallyConsistentBucketingSink<T> setBufferFSConfig(org.apache.hadoop.conf.Configuration config) {
		this.consistentFsConfig = new Configuration();
		for (Map.Entry<String, String> entry : config) {
			consistentFsConfig.setString(entry.getKey(), entry.getValue());
		}
		return this;
	}

	public EventuallyConsistentBucketingSink<T> setEventuallyConsistentBucketer(EventuallyConsistentBucketer bucketer) {
		this.bucketer = bucketer;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		if (this.writerTemplate instanceof InputTypeConfigurable) {
			((InputTypeConfigurable) writerTemplate).setInputType(type, executionConfig);
		}
	}

	/**
	 * Sets the {@link Writer} to be used for writing the incoming elements to bucket files.
	 *
	 * @param writer The {@code Writer} to use.
	 */
	public EventuallyConsistentBucketingSink<T> setWriter(Writer<T> writer) {
		this.writerTemplate = writer;
		return this;
	}

	/**
	 * Sets the local directory where buffers will be written.
	 * The default is the local os specific temporary directory.
	 */
	public EventuallyConsistentBucketingSink<T> setBufferDirectory(String path) {
		this.consistentFsDir = path;
		return this;
	}

	/**
	 * Sets the name of the flagFile file for marking buckets as being
	 * available for exactly once reading. The default is {@code _DONE}.
	 * @param flag The name of the flagFile file to use.
	 */
	public EventuallyConsistentBucketingSink<T> setFlagFile(String flag) {
		this.flagFile = flag;
		return this;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		if (state == null) {
			state = new State<>(writerTemplate, partPrefix, getRuntimeContext().getIndexOfThisSubtask());
		}
	}

	@Override
	public void close() throws Exception {
		state.cleanup();
		remoteFileSystem.close();
	}

	@Override
	public void invoke(T value) throws Exception {
		if (state.isWriterOpen && state.currentBucket.writer.getPos() > batchSize) {
			state.closeFile();
		}

		if (!state.isWriterOpen) {
			state.openFile();
		}

		state.currentBucket.writer.write(value);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		if (state == null) {
			state = new State<>(writerTemplate, partPrefix, getRuntimeContext().getIndexOfThisSubtask());
		}
		try {
			state.initFileSystem(consistentFsConfig, consistentFsDir);
			initFileSystem();
		} catch (IOException e) {
			LOG.error("Error while creating FileSystem when initializing the status of the EventuallyConsistentBucketingSink.", e);
			throw new RuntimeException("Error while creating FileSystem when initializing the status of the EventuallyConsistentBucketingSink.", e);
		}

		ListStateDescriptor<String> completedBucketStateDesc = new ListStateDescriptor<String>("completed-buckets", BasicTypeInfo.STRING_TYPE_INFO);
		completedBucketState = context.getOperatorStateStore().getUnionListState(completedBucketStateDesc);

		if (context.isRestored() && getRuntimeContext().getIndexOfThisSubtask() == 0) {
			for (String flag : completedBucketState.get()) {
				Path flagPath = new Path(flag);

				// this may not be the first time
				// we've restarted from this checkpoint
				if (!remoteFileSystem.exists(flagPath)) {
					remoteFileSystem.create(flagPath).close();
				}
			}

			RemoteIterator<LocatedFileStatus> oldBufferFiles = state.fileOnPath();

			while (oldBufferFiles.hasNext()) {
				LocatedFileStatus bufferFile = oldBufferFiles.next();
				if (bufferFile.isFile() && bufferFile.getPath().getName().startsWith(partPrefix)) {
					try {
						state.consistentFileSystem.delete(bufferFile.getPath(), true);
					} catch (FileNotFoundException ignore) {
						LOG.info("Attempted to delete a non-existent file, ignoring");
					} catch (IOException e) {
						LOG.error("Error while deleting old buffer file when restoring the status of the EventuallyConsistentBucketingSink.", e);
						throw new RuntimeException("Error while deleting old buffer file when restoring the status of the EventuallyConsistentBucketingSink.", e);
					}
				}
			}
		}
	}

	/**
	 * Create a file system with the user-defined {@code HDFS} configuration.
	 * @throws IOException
	 */
	private void initFileSystem() throws IOException {
		if (remoteFileSystem != null) {
			return;
		}
		org.apache.hadoop.conf.Configuration hadoopConf = HadoopFileSystem.getHadoopConfiguration();
		if (fsConfig != null) {
			String disableCacheName = String.format("remoteFileSystem.%s.impl.disable.cache", new Path(basePath).toUri().getScheme());
			hadoopConf.setBoolean(disableCacheName, true);
			for (String key : fsConfig.keySet()) {
				hadoopConf.set(key, fsConfig.getString(key, null));
			}
		}

		remoteFileSystem = new Path(basePath).getFileSystem(hadoopConf);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		completedBucketState.clear();

		if (state.isWriterOpen) {
			state.closeFile();
		}

		Path bucketPath = bucketer.getEventualConsistencyPath(new Path(basePath), context.getCheckpointId(), context.getCheckpointTimestamp());
		state.rollBucket(context.getCheckpointId(), bucketPath);

		synchronized (state.pendingBuckets) {
			Iterator<Map.Entry<Long, BucketState<T>>> buckets = state.pendingBuckets.entrySet().iterator();

			while (buckets.hasNext()) {
				Map.Entry<Long, BucketState<T>> entry = buckets.next();
				final BucketState<T> bucket = entry.getValue();

				if (bucket.status == UploadStatus.Completed) {
					continue;
				}

				bucket.status = UploadStatus.InProgress;

				Iterator<Path> files = bucket.bufferFiles.iterator();
				while (files.hasNext()) {
					Path file = files.next();
					remoteFileSystem.copyFromLocalFile(file, new Path(bucket.remotePath, file.getName()));

					Path uploaded = file.suffix("." + Long.toString(context.getCheckpointId()) + ".uploaded");
					state.consistentFileSystem.rename(file, uploaded);

					synchronized (bucket.uploadedFiles) {
						bucket.uploadedFiles.add(uploaded);
						files.remove();
					}
				}

				bucket.status = UploadStatus.Completed;

				synchronized (state.completedBuckets) {
					state.completedBuckets.put(entry.getKey(), bucket);
				}

				if (state.subtaskIndex == 0) {
					// only store a single instance per job
					Path flagFilePath = new Path(bucket.remotePath, flagFile);
					bucket.flagFile = flagFilePath;
					completedBucketState.add(flagFilePath.toString());
				}

				buckets.remove();
			}
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (state.completedBuckets) {
			Iterator<Map.Entry<Long, BucketState<T>>> buckets = state.completedBuckets.entrySet().iterator();

			while (buckets.hasNext()) {
				Map.Entry<Long, BucketState<T>> entry = buckets.next();

				if (checkpointId < entry.getKey()) {
					continue;
				}

				Iterator<Path> localFiles = entry.getValue().uploadedFiles.iterator();

				while (localFiles.hasNext()) {
					Path file = localFiles.next();
					state.consistentFileSystem.delete(file, true);
					localFiles.remove();
				}

				if (state.subtaskIndex == 0) {
					remoteFileSystem.create(entry.getValue().flagFile, true).close();
				}

				buckets.remove();
			}
		}
	}

	enum UploadStatus implements Serializable {
		NotStarted,
		InProgress,
		Completed
	}

	static class State<T> implements Serializable {
		private static final long serialVersionUID = 1L;

		private transient boolean isWriterOpen;

		private transient BucketState<T> currentBucket;

		private final transient Map<Long, BucketState<T>> pendingBuckets;

		private final transient Map<Long, BucketState<T>> completedBuckets;

		private transient FileSystem consistentFileSystem;

		private transient Path consistentFsPath;

		private Writer<T> writerTemplate;

		private int subtaskIndex;

		String partPrefix;

		State(Writer<T> writerTemplate, String partPrefix, int subtaskIndex) {
			this.isWriterOpen = false;
			this.currentBucket = new BucketState<>();
			this.pendingBuckets = new HashMap<>();
			this.completedBuckets = new HashMap<>();

			this.writerTemplate = writerTemplate;
			this.subtaskIndex = subtaskIndex;
			this.partPrefix = partPrefix;
		}

		private void openFile() throws IOException {
			if (isWriterOpen) {
				throw new IOException("Invalid operation; cannot open a file if the writer is already open");
			}

			isWriterOpen = true;
			currentBucket.open(writerTemplate, consistentFsPath, partPrefix, subtaskIndex, consistentFileSystem);
		}

		private void closeFile() throws IOException {
			if (!isWriterOpen) {
				throw new IOException("Invalid operation; cannot close a file that is not open");
			}

			currentBucket.close();
			isWriterOpen = false;
		}

		private void rollBucket(long checkpointId, Path remotePath) {
			Preconditions.checkArgument(!isWriterOpen, "Cannot roll a bucket if it has an open writer");

			synchronized (pendingBuckets) {
				currentBucket.remotePath = remotePath;
				pendingBuckets.put(checkpointId, currentBucket);
				currentBucket = new BucketState<>();
			}
		}

		RemoteIterator<LocatedFileStatus> fileOnPath() throws IOException {
			return consistentFileSystem.listFiles(consistentFsPath, true);
		}

		/**
		 * Create a buffer file system with the user-defined {@code HDFS} configuration.
		 * @throws IOException
		 */
		private void initFileSystem(Configuration consistentFsConfig, String consistentFsDir) throws IOException {
			if (consistentFileSystem != null) {
				return;
			}

			org.apache.hadoop.conf.Configuration hadoopConf = HadoopFileSystem.getHadoopConfiguration();
			if (consistentFsConfig != null) {
				String disableCacheName = String.format("remoteFileSystem.%s.impl.disable.cache", new Path(consistentFsDir).toUri().getScheme());
				hadoopConf.setBoolean(disableCacheName, true);
				for (String key : consistentFsConfig.keySet()) {
					hadoopConf.set(key, consistentFsConfig.getString(key, null));
				}
			}

			consistentFsPath = new Path(consistentFsDir, "flink-ecs-dir/" + Integer.toString(subtaskIndex));
			consistentFileSystem = consistentFsPath.getFileSystem(hadoopConf);

			if (consistentFileSystem.exists(consistentFsPath)) {
				consistentFileSystem.delete(consistentFsPath, true);
			} else if (!consistentFileSystem.mkdirs(consistentFsPath)) {
				LOG.error("Failed to create buffer directory %s", consistentFsPath.toString());
				throw new RuntimeException(String.format("Failed to create buffer directory %s", consistentFsPath.toString()));
			}
		}

		private void cleanup() throws IOException {
			if (isWriterOpen) {
				currentBucket.close();
			}

			consistentFileSystem.delete(new Path(consistentFsPath, currentBucket.subdir), true);
			for (BucketState<T> bucket : pendingBuckets.values()) {
				consistentFileSystem.delete(new Path(consistentFsPath, bucket.subdir), true);
			}

			for (BucketState<T> bucket : completedBuckets.values()) {
				consistentFileSystem.delete(new Path(consistentFsPath, bucket.subdir), true);
			}

			consistentFileSystem.close();
		}
	}

	static class BucketState<T> implements Serializable {
		/**
		 * Closed buffer files that **have not** been uploaded to remote.
		 */
		final transient List<Path> bufferFiles;

		/**
		 * Closed buffer files that **have** been uploaded
		 * to remote but have not been deleted from local
		 * disk.
		 */
		final transient List<Path> uploadedFiles;

		/**
		 * The remote path for this checkpoint as designated by the bucketer.
		 */
		transient Path remotePath;

		/**
		 * The remote path to the flag
		 * file that will mark this
		 * bucket as consistent.
		 */
		transient Path flagFile;

		/**
		 * Upload status of this bucket.
		 */
		transient UploadStatus status;

		/**
		 * For counting the part files inside a bucket directory. Part files follow the pattern {@code "{part-prefix}-{subtask}-{count}"}.
		 * When creating new part files we increase the counter.
		 */
		int partCounter;


		/**
		 * The unique sub directory that
		 * scopes the files for this particular
		 * bucket.
		 */
		String subdir;


		/**
		 * The actual writer that we user for writing the part files.
		 */
		private transient Writer<T> writer;

		/**
		 * The currentBucket path being written to by the writer.
 		 */
		private transient Path currentPath;

		BucketState() {
			this.status = UploadStatus.NotStarted;
			this.bufferFiles = new ArrayList<>();
			this.uploadedFiles = new ArrayList<>();
			this.subdir = Long.toString(Math.abs(UUID.randomUUID().getMostSignificantBits()));
		}

		void open(Writer<T> writerTemplate, Path consistentFsPath, String partPrefix, int subtaskIndex, FileSystem consistentFileSystem) throws IOException {
			try {
				writer = writerTemplate.duplicate();
				String fileName = subdir + "/" + partPrefix + "-" + subtaskIndex + "-" + this.partCounter;
				currentPath = new Path(consistentFsPath, fileName);
				this.partCounter++;
				writer.open(consistentFileSystem, currentPath);
			} catch (IOException e) {
				LOG.error("Failed to open buffer file {} for writing", currentPath, e);
				throw new IOException("Failed to open buffer file " + currentPath + " for writing", e);
			}
		}

		void close() throws IOException {
			try {
				writer.close();
			} catch (IOException e) {
				LOG.error("Unable to close buffer part file in EventuallyConsistentBucketingSink", e);
				throw new IOException("Unable to close buffer part file in EventuallyConsistentBucketingSink", e);
			}

			bufferFiles.add(currentPath);
			currentPath = null;
		}
	}
}
