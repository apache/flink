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
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.tuple.Tuple2;
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
	extends RichSourceFunction<FileInputSplit> implements Checkpointed<Long> {

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

	private Long globalModificationTime;

	private transient Object checkpointLock;

	private volatile boolean isRunning = true;

	public ContinuousFileMonitoringFunction(
		FileInputFormat<OUT> format, String path,
		FileProcessingMode watchType,
		int readerParallelism, long interval) {

		if (watchType != FileProcessingMode.PROCESS_ONCE && interval < MIN_MONITORING_INTERVAL) {
			throw new IllegalArgumentException("The specified monitoring interval (" + interval + " ms) is " +
				"smaller than the minimum allowed one (100 ms).");
		}
		this.format = Preconditions.checkNotNull(format, "Unspecified File Input Format.");
		this.path = Preconditions.checkNotNull(path, "Unspecified Path.");

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

		checkpointLock = context.getCheckpointLock();
		switch (watchType) {
			case PROCESS_CONTINUOUSLY:
				while (isRunning) {
					synchronized (checkpointLock) {
						monitorDirAndForwardSplits(fileSystem, context);
					}
					Thread.sleep(interval);
				}

				// here we do not need to set the running to false and the
				// globalModificationTime to Long.MAX_VALUE because to arrive here,
				// either close() or cancel() have already been called, so this
				// is already done.

				break;
			case PROCESS_ONCE:
				synchronized (checkpointLock) {
					monitorDirAndForwardSplits(fileSystem, context);
					globalModificationTime = Long.MAX_VALUE;
					isRunning = false;
				}
				break;
			default:
				isRunning = false;
				throw new RuntimeException("Unknown WatchType" + watchType);
		}
	}

	private void monitorDirAndForwardSplits(FileSystem fs, SourceContext<FileInputSplit> context) throws IOException, JobException {
		assert (Thread.holdsLock(checkpointLock));

		List<Tuple2<Long, List<FileInputSplit>>> splitsByModTime = getInputSplitSortedOnModTime(fs);

		Iterator<Tuple2<Long, List<FileInputSplit>>> it = splitsByModTime.iterator();
		while (it.hasNext()) {
			forwardSplits(it.next(), context);
			it.remove();
		}
	}

	private void forwardSplits(Tuple2<Long, List<FileInputSplit>> splitsToFwd, SourceContext<FileInputSplit> context) {
		assert (Thread.holdsLock(checkpointLock));

		Long modTime = splitsToFwd.f0;
		List<FileInputSplit> splits = splitsToFwd.f1;

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
		assert (Thread.holdsLock(checkpointLock));
		boolean shouldIgnore = modificationTime <= globalModificationTime;
		if (shouldIgnore) {
			LOG.debug("Ignoring " + filePath + ", with mod time= " + modificationTime + " and global mod time= " + globalModificationTime);
		}
		return  shouldIgnore;
	}

	@Override
	public void close() throws Exception {
		super.close();
		synchronized (checkpointLock) {
			globalModificationTime = Long.MAX_VALUE;
			isRunning = false;
		}
		LOG.info("Closed File Monitoring Source.");
	}

	@Override
	public void cancel() {
		if (checkpointLock != null) {
			// this is to cover the case where cancel() is called before the run()
			synchronized (checkpointLock) {
				globalModificationTime = Long.MAX_VALUE;
				isRunning = false;
			}
		} else {
			globalModificationTime = Long.MAX_VALUE;
			isRunning = false;
		}
	}

	//	---------------------			Checkpointing			--------------------------

	@Override
	public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		return globalModificationTime;
	}

	@Override
	public void restoreState(Long state) throws Exception {
		this.globalModificationTime = state;
	}
}
