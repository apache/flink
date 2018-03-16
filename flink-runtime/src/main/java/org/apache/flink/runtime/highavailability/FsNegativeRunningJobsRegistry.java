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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This {@link RunningJobsRegistry} tracks the status jobs via marker files,
 * marking running jobs via running marker files, marking finished jobs via finished marker files.
 * 
 * <p>The general contract is the following:
 * <ul>
 *     <li>Initially, a marker file does not exist (no one created it, yet), which means
 *         the specific job is pending.</li>
 *     <li>The first JobManager that granted leadership calls this service to create the running marker file,
 *         which marks the job as running.</li>
 *     <li>If a JobManager gains leadership but sees the running marker file,
 *         it will realize that the job has been scheduled already and needs reconciling.</li>
 *     <li>The JobManager that finishes calls this service to create the marker file,
 *         which marks the job as finished.</li>
 *     <li>If a JobManager gains leadership at some point when shutdown is in progress,
 *         it will see the marker file and realize that the job is finished.</li>
 *     <li>The application framework is expected to clean the file once the application
 *         is completely shut down. At that point, no JobManager will attempt to
 *         start the job, even if it gains leadership.</li>
 * </ul>
 * 
 * <p>This registry is especially tailored towards deployment modes like for example
 * YARN, where HDFS is available as a persistent file system, and the YARN
 * application's working directories on HDFS are automatically cleaned
 * up after the application completed. 
 */
public class FsNegativeRunningJobsRegistry implements RunningJobsRegistry {

	private static final String DONE_PREFIX = ".job_complete_";

	private static final String RUNNING_PREFIX = ".job_runing_";

	private final FileSystem fileSystem;

	private final Path basePath;

	/**
	 * Creates a new registry that writes to the FileSystem and working directory
	 * denoted by the given path.
	 * 
	 * <p>The initialization will attempt to write to the given working directory, in
	 * order to catch setup/configuration errors early.
	 *
	 * @param workingDirectory The working directory for files to track the job status.
	 *
	 * @throws IOException Thrown, if the specified directory cannot be accessed.
	 */
	public FsNegativeRunningJobsRegistry(Path workingDirectory) throws IOException {
		this(workingDirectory.getFileSystem(), workingDirectory);
	}

	/**
	 * Creates a new registry that writes its files to the given FileSystem at
	 * the given working directory path.
	 * 
	 * <p>The initialization will attempt to write to the given working directory, in
	 * order to catch setup/configuration errors early.
	 *
	 * @param fileSystem The FileSystem to use for the marker files.
	 * @param workingDirectory The working directory for files to track the job status.
	 *
	 * @throws IOException Thrown, if the specified directory cannot be accessed.
	 */
	public FsNegativeRunningJobsRegistry(FileSystem fileSystem, Path workingDirectory) throws IOException {
		this.fileSystem = checkNotNull(fileSystem, "fileSystem");
		this.basePath = checkNotNull(workingDirectory, "workingDirectory");

		// to be safe, attempt to write to the working directory, to
		// catch problems early
		final Path testFile = new Path(workingDirectory, ".registry_test");
		try {
			createFile(testFile, false);
		}
		catch (IOException e) {
			throw new IOException("Unable to write to working directory: " + workingDirectory, e);
		}
		finally {
			fileSystem.delete(testFile, false);
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public void setJobRunning(JobID jobID) throws IOException {
		checkNotNull(jobID, "jobID");
		final Path filePath = createMarkerFilePath(RUNNING_PREFIX, jobID);

		// create the file
		// to avoid an exception if the job already exists, set overwrite=true
		createFile(filePath, true);
	}

	@Override
	public void setJobFinished(JobID jobID) throws IOException {
		checkNotNull(jobID, "jobID");
		final Path filePath = createMarkerFilePath(DONE_PREFIX, jobID);

		// create the file
		// to avoid an exception if the job already exists, set overwrite=true
		createFile(filePath, true);
	}

	@Override
	public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException {
		checkNotNull(jobID, "jobID");

		// first check for the existence of the complete file
		if (fileSystem.exists(createMarkerFilePath(DONE_PREFIX, jobID))) {
			// complete file was found --> job is terminated
			return JobSchedulingStatus.DONE;
		}
		// check for the existence of the running file
		else if (fileSystem.exists(createMarkerFilePath(RUNNING_PREFIX, jobID))) {
			// running file was found --> job is terminated
			return JobSchedulingStatus.RUNNING;
		}
		else {
			// file does not exist, job is not scheduled
			return JobSchedulingStatus.PENDING;
		}
	}

	@Override
	public void clearJob(JobID jobID) throws IOException {
		checkNotNull(jobID, "jobID");
		final Path runningFilePath = createMarkerFilePath(RUNNING_PREFIX, jobID);
		final Path doneFilePath = createMarkerFilePath(DONE_PREFIX, jobID);

		// delete the running marker file, if it exists
		try {
			fileSystem.delete(runningFilePath, false);
		}
		catch (FileNotFoundException ignored) {}

		// delete the finished marker file, if it exists
		try {
			fileSystem.delete(doneFilePath, false);
		}
		catch (FileNotFoundException ignored) {}
	}

	private Path createMarkerFilePath(String prefix, JobID jobId) {
		return new Path(basePath, prefix + jobId.toString());
	}

	private void createFile(Path path, boolean overwrite) throws IOException {
		final WriteMode writeMode = overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE;

		try (FSDataOutputStream out = fileSystem.create(path, writeMode)) {
			out.write(42);
		}
	}
}
