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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.JobException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This is the single (non-parallel) task which monitors a user-provided path and assigns splits
 * to downstream tasks for further reading and processing. Which splits will be further processed
 * depends on the user-provided {@link FileSplitMonitoringFunction.WatchType}.
 */
@Internal
public class FileSplitMonitoringFunction<OUT>
	extends RichSourceFunction<FileInputSplit> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(FileSplitMonitoringFunction.class);

	/**
	 * Specifies when computation will be triggered.
	 */
	public enum WatchType {
		REPROCESS_WITH_APPENDED		// Reprocesses the whole file when new data is appended.
	}

	/** The path to monitor. */
	private final String path;

	/** The default parallelism for the job, as this is going to be the parallelism of the downstream readers. */
	private final int readerParallelism;

	/** The {@link FileInputFormat} to be read. */
	private FileInputFormat<OUT> format;

	/** How often to monitor the state of the directory for new data. */
	private final long interval;

	/** Which new data to process (see {@link WatchType}. */
	private final WatchType watchType;

	private long globalModificationTime;

	private FilePathFilter pathFilter;

	private volatile boolean isRunning = true;

	/**
	 * This is the {@link Configuration} to be used to initialize the input format at the reader
	 * (see {@link #open(Configuration)}). In the codebase, whenever {@link #open(Configuration)} is called,
	 * it is passed a new configuration, thus ignoring potential user-specified parameters. Now, we pass a
	 * configuration object at the constructor, which is shipped to the remote tasks.
	 * */
	private Configuration configuration;

	public FileSplitMonitoringFunction(
		FileInputFormat<OUT> format, String path, Configuration configuration,
		WatchType watchType, int readerParallelism, long interval) {

		this(format, path, configuration, FilePathFilter.DefaultFilter.getInstance(), watchType, readerParallelism, interval);
	}

	public FileSplitMonitoringFunction(
		FileInputFormat<OUT> format, String path, Configuration configuration,
		FilePathFilter filter, WatchType watchType, int readerParallelism, long interval) {

		this.format = Preconditions.checkNotNull(format);
		this.path = Preconditions.checkNotNull(path);
		this.configuration = Preconditions.checkNotNull(configuration);

		Preconditions.checkArgument(interval >= 100,
			"The specified monitoring interval is smaller than the minimum allowed one (100 ms).");
		this.interval = interval;

		this.watchType = watchType;

		this.pathFilter = Preconditions.checkNotNull(filter);

		this.readerParallelism = Math.max(readerParallelism, 1);
		this.globalModificationTime = Long.MIN_VALUE;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		format.configure(this.configuration);
	}

	@Override
	public void run(SourceFunction.SourceContext<FileInputSplit> context) throws Exception {
		FileSystem fileSystem = FileSystem.get(new URI(path));
		while (isRunning) {
			monitor(fileSystem, context);
			Thread.sleep(interval);
		}
	}

	private void monitor(FileSystem fs, SourceContext<FileInputSplit> context) throws IOException, JobException {
		List<FileStatus> files = listEligibleFiles(fs);
		for (FileInputSplit split : getInputSplits(files)) {
			processSplit(split, context);
		}
	}

	private void processSplit(FileInputSplit split, SourceContext<FileInputSplit> context) {
		switch (watchType) {
			case REPROCESS_WITH_APPENDED:
				context.collect(split);
				break;
			default:
				throw new RuntimeException("Unknown WatchType" + watchType);
		}
	}

	/**
	 * Creates the input splits for the path to be assigned to the downstream tasks.
	 * Those are going to read their contents for further processing. Splits belonging
	 * to files in the {@code files} list are ignored.
	 * @param files The files to ignore.
	 */
	private Set<FileInputSplit> getInputSplits(List<FileStatus> files) throws IOException {
		if (files.isEmpty()) {
			return new HashSet<>();
		}

		FileInputSplit[] inputSplits = format.createInputSplits(readerParallelism);

		Set<FileInputSplit> splitsToForward = new HashSet<>();
		for (FileInputSplit split: inputSplits) {
			for(FileStatus file: files) {
				if (file.getPath().equals(split.getPath())) {
					splitsToForward.add(split);
					break;
				}
			}
		}
		return splitsToForward;
	}

	/**
	 * Returns the files that have data to be processed. This method returns the
	 * Paths to the aforementioned files. It is up to the {@link #processSplit(FileInputSplit, SourceContext)}
	 * method to decide which parts of the file to be processed, and forward them downstream.
	 */
	private List<FileStatus> listEligibleFiles(FileSystem fileSystem) throws IOException {
		List<FileStatus> files = new ArrayList<>();

		FileStatus[] statuses = fileSystem.listStatus(new Path(path));
		long maxModificationTime = Long.MIN_VALUE;
		if (statuses == null) {
			LOG.warn("Path does not exist: {}", path);
		} else {
			// handle the new files
			for (FileStatus status : statuses) {
				Path filePath = status.getPath();
				long modificationTime = status.getModificationTime();
				if (!shouldIgnore(filePath, modificationTime)) {
					files.add(status);
					if (modificationTime > maxModificationTime) {
						maxModificationTime = modificationTime;
					}
				}
			}
		}

		// after finding the eligible files to process, update the
		// global max modification time seen so far to reflect the
		// latest monitoring round.
		if (maxModificationTime > globalModificationTime) {
			globalModificationTime = maxModificationTime;
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
		return (pathFilter != null && pathFilter.filterPath(filePath)) || modificationTime <= globalModificationTime;
	}

	@Override
	public void close() throws Exception {
		super.close();
		format.close();
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
