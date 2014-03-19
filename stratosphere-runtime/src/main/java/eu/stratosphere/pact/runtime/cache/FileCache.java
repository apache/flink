/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.cache;

import eu.stratosphere.api.common.cache.DistributedCache;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.fs.local.LocalFileSystem;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.runtime.ExecutorThreadFactory;
import eu.stratosphere.nephele.util.IOUtils;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * FileCache is used to create the local tmp file for the registered cache file when a task is deployed. Also when the
 * task is unregistered, it will remove the local tmp file. Given that another task from the same job may be registered
 * shortly after, there exists a 5 second delay	before clearing the local tmp file.
 */
public class FileCache {

	private LocalFileSystem lfs = new LocalFileSystem();

	private Map<Pair<JobID, String>, Integer> count = new HashMap<Pair<JobID,String>, Integer>();

	private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10, ExecutorThreadFactory.INSTANCE);

	/**
	 * If the file doesn't exists locally, it will copy the file to the temp directory.
	 */
	public FutureTask<Path> createTmpFile(String name, String filePath, JobID jobID) {

		synchronized (count) {
			Pair<JobID, String> key = new ImmutablePair(jobID,name);
			if (count.containsKey(key)) {
				count.put(key, count.get(key) + 1);
			} else {
				count.put(key, 1);
			}
		}
		CopyProcess cp = new CopyProcess(name, filePath, jobID);
		FutureTask<Path> copyTask = new FutureTask<Path>(cp);
		executorService.submit(copyTask);
		return copyTask;
	}

	/**
	 * Leave a 5 seconds delay to clear the local file.
	 */
	public void deleteTmpFile(String name, JobID jobID) {
		DeleteProcess dp = new DeleteProcess(name, jobID, count.get(new ImmutablePair(jobID,name)));
		executorService.schedule(dp, 5000L, TimeUnit.MILLISECONDS);
	}

	public Path getTempDir(JobID jobID, String name) {
		return new Path(GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH), DistributedCache.TMP_PREFIX + jobID.toString() + "_" +  name);
	}

	public void shutdown() {
		if (this.executorService != null) {
			this.executorService.shutdown();
			try {
				this.executorService.awaitTermination(5000L, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				throw new RuntimeException("Error shutting down the file cache", e);
			}
		}
	}

	/**
	 * Asynchronous file copy process
	 */
	private class CopyProcess implements Callable<Path> {
		private JobID jobID;
		private String name;
		private String filePath;

		public CopyProcess(String name, String filePath, JobID jobID) {
			this.name = name;
			this.filePath = filePath;
			this.jobID = jobID;
		}
		public Path call()  {
			Path tmp = getTempDir(jobID, name);
			try {
				if (!lfs.exists(tmp)) {
					FSDataOutputStream lfsOutput = lfs.create(tmp, false);
					Path distributedPath = new Path(filePath);
					FileSystem fs = distributedPath.getFileSystem();
					FSDataInputStream fsInput = fs.open(distributedPath);
					IOUtils.copyBytes(fsInput, lfsOutput);
				}
			} catch (IOException e1) {
				throw new RuntimeException("Error copying a file from hdfs to the local fs", e1);
			}
			return tmp;
		}
	}
	/**
	 * If no task is using this file after 5 seconds, clear it.
	 */
	private class DeleteProcess implements Runnable {
		private String name;
		private JobID jobID;
		private int oldCount;

		public DeleteProcess(String name, JobID jobID, int c) {
			this.name = name;
			this.jobID = jobID;
			this.oldCount = c;
		}

		public void run() {
			synchronized (count) {
				if (count.get(new ImmutablePair(jobID, name)) != oldCount) {
					return;
				}
			}
			Path tmp = getTempDir(jobID, name);
			try {
				if (lfs.exists(tmp)) {
					lfs.delete(tmp, true);
				}
			} catch (IOException e1) {
				throw new RuntimeException("Error deleting the file", e1);
			}
		}
	}
}
