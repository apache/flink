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
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import eu.stratosphere.api.common.cache.DistributedCache.DistributedCacheEntry;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.fs.local.LocalFileSystem;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.runtime.ExecutorThreadFactory;
import eu.stratosphere.nephele.util.IOUtils;

/**
 * The FileCache is used to create the local files for the registered cache files when a task is deployed. 
 * The files will be removed when the task is unregistered after a 5 second delay.
 * A given file x will be placed in "<system-tmp-dir>/tmp_<jobID>/".
 */
public class FileCache {

	private LocalFileSystem lfs = new LocalFileSystem();

	private Map<Pair<JobID, String>, Integer> count = new HashMap<Pair<JobID,String>, Integer>();

	private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10, ExecutorThreadFactory.INSTANCE);

	/**
	 * If the file doesn't exists locally, it will copy the file to the temp directory.
	 * @param name file identifier
	 * @param entry entry containing all relevant information
	 * @param jobID
	 * @return copy task
	 */
	public FutureTask<Path> createTmpFile(String name, DistributedCacheEntry entry, JobID jobID) {
		synchronized (count) {
			Pair<JobID, String> key = new ImmutablePair(jobID, name);
			if (count.containsKey(key)) {
				count.put(key, count.get(key) + 1);
			} else {
				count.put(key, 1);
			}
		}
		CopyProcess cp = new CopyProcess(name, entry, jobID);
		FutureTask<Path> copyTask = new FutureTask<Path>(cp);
		executorService.submit(copyTask);
		return copyTask;
	}

	/**
	 * Deletes the local file after a 5 second delay.
	 * @param name file identifier
	 * @param entry entry containing all relevant information
	 * @param jobID
	 */
	public void deleteTmpFile(String name, DistributedCacheEntry entry, JobID jobID) {
		DeleteProcess dp = new DeleteProcess(name, entry, jobID, count.get(new ImmutablePair(jobID,name)));
		executorService.schedule(dp, 5000L, TimeUnit.MILLISECONDS);
	}

	public Path getTempDir(JobID jobID, String childPath) {
		return new Path(GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH), DistributedCache.TMP_PREFIX + jobID.toString() + "/" + childPath);
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
		private Boolean executable;

		public CopyProcess(String name, DistributedCacheEntry e, JobID jobID) {
			this.name = name;
			this.filePath = e.filePath;
			this.executable = e.isExecutable;
			this.jobID = jobID;
		}
		@Override
		public Path call()  {
			Path tmp = getTempDir(jobID, filePath.substring(filePath.lastIndexOf("/") + 1));
			try {
				create(new Path(filePath), tmp);
			} catch (IOException e1) {
				throw new RuntimeException("Error copying a file from hdfs to the local fs", e1);
			}
			return tmp;
		}
		
		private void create(Path distributedFilePath, Path localFilePath) throws IOException {
			if (!lfs.exists(localFilePath)) {
				FileSystem dfs = distributedFilePath.getFileSystem();
				if (dfs.getFileStatus(distributedFilePath).isDir()) {
					lfs.mkdirs(localFilePath);
					FileStatus[] contents = dfs.listStatus(distributedFilePath);
					for (FileStatus content : contents) {
						String distPath = content.getPath().toString();
						if (content.isDir()){
							distPath = distPath.substring(0,distPath.length() - 1);
						}
						String localPath = localFilePath.toString() + distPath.substring(distPath.lastIndexOf("/"));
						create(content.getPath(), new Path(localPath));
					}
				} else {
					FSDataOutputStream lfsOutput = lfs.create(localFilePath, false);
					FSDataInputStream fsInput = dfs.open(distributedFilePath);
					IOUtils.copyBytes(fsInput, lfsOutput);
					new File(localFilePath.toString()).setExecutable(executable);
				}
			}
		}
	}

	/**
	 * If no task is using this file after 5 seconds, clear it.
	 */
	private class DeleteProcess implements Runnable {
		private String name;
		private String filePath;
		private JobID jobID;
		private int oldCount;

		public DeleteProcess(String name, DistributedCacheEntry e, JobID jobID, int c) {
			this.name = name;
			this.filePath = e.filePath;
			this.jobID = jobID;
			this.oldCount = c;
		}
		@Override
		public void run() {
			synchronized (count) {
				if (count.get(new ImmutablePair(jobID, name)) != oldCount) {
					return;
				}
			}
			Path tmp = getTempDir(jobID, "");
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
