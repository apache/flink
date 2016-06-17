/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.JobException;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This is the single (non-parallel) task which takes a {@link FileInputFormat} and is responsible for
 * i) monitoring a user-provided path, ii) deciding which files should be further read and processed,
 * iii) creating the {@link FileInputSplit FileInputSplits} corresponding to those files, and iv) assigning
 * them to downstream tasks for further reading and processing. Which splits will be further processed
 * depends on the user-provided {@link FileProcessingMode} and the {@link FilePathFilter}.
 * The splits of the files to be read are then forwarded to the downstream
 * {@link ContinuousFileReaderOperator} which can have parallelism greater than one.
 */
@Internal
public class ContinuousFileMonitoringFunction<OUT>
	extends RichSourceFunction<FileInputSplit> implements Checkpointed<Tuple3<List<Tuple2<Long, List<FileInputSplit>>>, Tuple2<Long, List<FileInputSplit>>, Long>> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileMonitoringFunction.class);

	/**
	 * The minimum interval allowed between consecutive path scans. This is applicable if the
	 * {@code watchType} is set to {@code PROCESS_CONTINUOUSLY}.
	 */
	public static final long MIN_MONITORING_INTERVAL = 100l;

	/** The path to monitor. */
	private final String path;

	/** The default parallelism for the job, as this is going to be the parallelism of the downstream readers. */
	private final int readerParallelism;

	/** The {@link FileInputFormat} to be read. */
	private FileInputFormat<OUT> format;

	/** How often to monitor the state of the directory for new data. */
	private final long interval;

	/** Which new data to process (see {@link FileProcessingMode}. */
	private final FileProcessingMode watchType;

	private List<Tuple2<Long, List<FileInputSplit>>> splitsToFwdOrderedAscByModTime;

	private Tuple2<Long, List<FileInputSplit>> currentSplitsToFwd;

	private long globalModificationTime;

	private FilePathFilter pathFilter;

	private volatile boolean isRunning = true;

	public ContinuousFileMonitoringFunction(
		FileInputFormat<OUT> format, String path,
		FilePathFilter filter, FileProcessingMode watchType,
		int readerParallelism, long interval) {

		if (watchType != FileProcessingMode.PROCESS_ONCE && interval < MIN_MONITORING_INTERVAL) {
			throw new IllegalArgumentException("The specified monitoring interval (" + interval + " ms) is " +
				"smaller than the minimum allowed one (100 ms).");
		}
		this.format = Preconditions.checkNotNull(format, "Unspecified File Input Format.");
		this.path = Preconditions.checkNotNull(path, "Unspecified Path.");
		this.pathFilter = Preconditions.checkNotNull(filter, "Unspecified File Path Filter.");

		this.interval = interval;
		this.watchType = watchType;
		this.readerParallelism = Math.max(readerParallelism, 1);
		this.globalModificationTime = Long.MIN_VALUE;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(Configuration parameters) throws Exception {
		LOG.info("Opening File Monitoring Source.");
		
		super.open(parameters);
		format.configure(parameters);
	}

	@Override
	public void run(SourceFunction.SourceContext<FileInputSplit> context) throws Exception {
		FileSystem fileSystem = FileSystem.get(new URI(path));

		switch (watchType) {
			case PROCESS_CONTINUOUSLY:
				while (isRunning) {
					monitorDirAndForwardSplits(fileSystem, context);
					Thread.sleep(interval);
				}
				isRunning = false;
				break;
			case PROCESS_ONCE:
				monitorDirAndForwardSplits(fileSystem, context);
				isRunning = false;
				break;
			default:
				isRunning = false;
				throw new RuntimeException("Unknown WatchType" + watchType);
		}
	}

	private void monitorDirAndForwardSplits(FileSystem fs, SourceContext<FileInputSplit> context) throws IOException, JobException {
		final Object lock = context.getCheckpointLock();

		// it may be non-null in the case of a recovery after a failure.
		if (currentSplitsToFwd != null) {
			synchronized (lock) {
				forwardSplits(currentSplitsToFwd, context);
			}
		}
		currentSplitsToFwd = null;

		// it may be non-null in the case of a recovery after a failure.
		if (splitsToFwdOrderedAscByModTime == null) {
			splitsToFwdOrderedAscByModTime = getInputSplitSortedOnModTime(fs);
		}

		Iterator<Tuple2<Long, List<FileInputSplit>>> it =
			splitsToFwdOrderedAscByModTime.iterator();

		while (it.hasNext()) {
			synchronized (lock) {
				currentSplitsToFwd = it.next();
				it.remove();
				forwardSplits(currentSplitsToFwd, context);
			}
		}

		// set them to null to distinguish from a restore.
		splitsToFwdOrderedAscByModTime = null;
		currentSplitsToFwd = null;
	}

	private void forwardSplits(Tuple2<Long, List<FileInputSplit>> splitsToFwd, SourceContext<FileInputSplit> context) {
		currentSplitsToFwd = splitsToFwd;
		Long modTime = currentSplitsToFwd.f0;
		List<FileInputSplit> splits = currentSplitsToFwd.f1;

		Iterator<FileInputSplit> it = splits.iterator();
		while (it.hasNext()) {
			FileInputSplit split = it.next();
			processSplit(split, context);
			it.remove();
		}

		// update the global modification time
		if (modTime >= globalModificationTime) {
			globalModificationTime = modTime;
		}
	}

	private void processSplit(FileInputSplit split, SourceContext<FileInputSplit> context) {
		LOG.info("Forwarding split: " + split);
		context.collect(split);
	}

	private List<Tuple2<Long, List<FileInputSplit>>> getInputSplitSortedOnModTime(FileSystem fileSystem) throws IOException {
		List<FileStatus> eligibleFiles = listEligibleFiles(fileSystem);
		if (eligibleFiles.isEmpty()) {
			return new ArrayList<>();
		}

		Map<Long, List<FileInputSplit>> splitsToForward = getInputSplits(eligibleFiles);
		List<Tuple2<Long, List<FileInputSplit>>> sortedSplitsToForward = new ArrayList<>();

		for (Map.Entry<Long, List<FileInputSplit>> entry : splitsToForward.entrySet()) {
			sortedSplitsToForward.add(new Tuple2<>(entry.getKey(), entry.getValue()));
		}

		Collections.sort(sortedSplitsToForward, new Comparator<Tuple2<Long, List<FileInputSplit>>>() {
			@Override
			public int compare(Tuple2<Long, List<FileInputSplit>> o1, Tuple2<Long, List<FileInputSplit>> o2) {
				return (int) (o1.f0 - o2.f0);
			}
		});

		return sortedSplitsToForward;
	}

	/**
	 * Creates the input splits for the path to be forwarded to the downstream tasks of the
	 * {@link ContinuousFileReaderOperator}. Those tasks are going to read their contents for further
	 * processing. Splits belonging to files in the {@code eligibleFiles} list are the ones
	 * that are shipped for further processing.
	 * @param eligibleFiles The files to process.
	 */
	private Map<Long, List<FileInputSplit>> getInputSplits(List<FileStatus> eligibleFiles) throws IOException {
		if (eligibleFiles.isEmpty()) {
			return new HashMap<>();
		}

		FileInputSplit[] inputSplits = format.createInputSplits(readerParallelism);

		Map<Long, List<FileInputSplit>> splitsPerFile = new HashMap<>();
		for (FileInputSplit split: inputSplits) {
			for (FileStatus file: eligibleFiles) {
				if (file.getPath().equals(split.getPath())) {
					Long modTime = file.getModificationTime();

					List<FileInputSplit> splitsToForward = splitsPerFile.get(modTime);
					if (splitsToForward == null) {
						splitsToForward = new LinkedList<>();
						splitsPerFile.put(modTime, splitsToForward);
					}
					splitsToForward.add(split);
					break;
				}
			}
		}
		return splitsPerFile;
	}

	/**
	 * Returns the files that have data to be processed. This method returns the
	 * Paths to the aforementioned files. It is up to the {@link #processSplit(FileInputSplit, SourceContext)}
	 * method to decide which parts of the file to be processed, and forward them downstream.
	 */
	private List<FileStatus> listEligibleFiles(FileSystem fileSystem) throws IOException {
		List<FileStatus> files = new ArrayList<>();

		FileStatus[] statuses = fileSystem.listStatus(new Path(path));
		if (statuses == null) {
			LOG.warn("Path does not exist: {}", path);
		} else {
			// handle the new files
			for (FileStatus status : statuses) {
				Path filePath = status.getPath();
				long modificationTime = status.getModificationTime();
				if (!shouldIgnore(filePath, modificationTime)) {
					files.add(status);
				}
			}
		}
		return files;
	}

	/**
	 * Returns {@code true} if the file is NOT to be processed further.
	 * This happens in the following cases:
	 *
	 * If the user-specified path filtering method returns {@code true} for the file,
	 * or if the modification time of the file is smaller than the {@link #globalModificationTime}, which
	 * is the time of the most recent modification found in any of the already processed files.
	 */
	private boolean shouldIgnore(Path filePath, long modificationTime) {
		boolean shouldIgnore = ((pathFilter != null && pathFilter.filterPath(filePath)) || modificationTime <= globalModificationTime);
		if (shouldIgnore) {
			LOG.debug("Ignoring " + filePath + ", with mod time= " + modificationTime + " and global mod time= " + globalModificationTime);
		}
		return  shouldIgnore;
	}

	@Override
	public void close() throws Exception {
		super.close();
		isRunning = false;
		LOG.info("Closed File Monitoring Source.");
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	//	---------------------			Checkpointing			--------------------------

	@Override
	public Tuple3<List<Tuple2<Long, List<FileInputSplit>>>, Tuple2<Long, List<FileInputSplit>>, Long> snapshotState(
		long checkpointId, long checkpointTimestamp) throws Exception {

		if (!isRunning) {
			LOG.debug("snapshotState() called on closed source");
			return null;
		}
		return new Tuple3<>(splitsToFwdOrderedAscByModTime,
			currentSplitsToFwd, globalModificationTime);
	}

	@Override
	public void restoreState(Tuple3<List<Tuple2<Long, List<FileInputSplit>>>,
		Tuple2<Long, List<FileInputSplit>>, Long> state) throws Exception {

		this.splitsToFwdOrderedAscByModTime = state.f0;
		this.currentSplitsToFwd = state.f1;
		this.globalModificationTime = state.f2;
	}
}
