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

package org.apache.flink.runtime.filecache;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The FileCache is used to create the local files for the registered cache files when a task is deployed. 
 * The files will be removed when the task is unregistered after a 5 second delay.
 * A given file x will be placed in "<system-tmp-dir>/tmp_<jobID>/".
 */
public class FileCache {

	private static final Logger LOG = LoggerFactory.getLogger(FileCache.class);
	
	private static final Object lock = new Object();
	
	
	private LocalFileSystem lfs = new LocalFileSystem();

	private Map<JobID, Map<String, Integer>> jobCounts = new HashMap<JobID, Map<String, Integer>>();

	private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10, ExecutorThreadFactory.INSTANCE);

	/**
	 * If the file doesn't exists locally, it will copy the file to the temp directory.
	 * @param name file identifier
	 * @param entry entry containing all relevant information
	 * @param jobID
	 * @return copy task
	 */
	public FutureTask<Path> createTmpFile(String name, DistributedCacheEntry entry, JobID jobID) {
		synchronized (lock) {
			if (!jobCounts.containsKey(jobID)) {
				jobCounts.put(jobID, new HashMap<String, Integer>());
			}
			Map<String, Integer> count = jobCounts.get(jobID);
			if (count.containsKey(name)) {
				count.put(name, count.get(name) + 1);
			} else {
				count.put(name, 1);
			}
		}
		CopyProcess cp = new CopyProcess(entry, jobID);
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
		DeleteProcess dp = new DeleteProcess(name, entry, jobID);
		executorService.schedule(dp, 5000L, TimeUnit.MILLISECONDS);
	}

	public Path getTempDir(JobID jobID, String childPath) {
		return new Path(GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH), DistributedCache.TMP_PREFIX + jobID.toString() + "/" + childPath);
	}

	public void shutdown() {
		ScheduledExecutorService es = this.executorService;
		if (es != null) {
			es.shutdown();
			try {
				es.awaitTermination(5000L, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				throw new RuntimeException("Error shutting down the file cache", e);
			}
		}
	}

	public static void copy(Path sourcePath, Path targetPath, boolean executable) throws IOException {
		FileSystem sFS = sourcePath.getFileSystem();
		FileSystem tFS = targetPath.getFileSystem();
		if (!tFS.exists(targetPath)) {
			if (sFS.getFileStatus(sourcePath).isDir()) {
				tFS.mkdirs(targetPath);
				FileStatus[] contents = sFS.listStatus(sourcePath);
				for (FileStatus content : contents) {
					String distPath = content.getPath().toString();
					if (content.isDir()) {
						if (distPath.endsWith("/")) {
							distPath = distPath.substring(0, distPath.length() - 1);
						}
					}
					String localPath = targetPath.toString() + distPath.substring(distPath.lastIndexOf("/"));
					copy(content.getPath(), new Path(localPath), executable);
				}
			} else {
				try {
					FSDataOutputStream lfsOutput = tFS.create(targetPath, false);
					FSDataInputStream fsInput = sFS.open(sourcePath);
					IOUtils.copyBytes(fsInput, lfsOutput);
					new File(targetPath.toString()).setExecutable(executable);
				} catch (IOException ioe) {
					LOG.error("could not copy file to local file cache.", ioe);
				}
			}
		}
	}

	/**
	 * Asynchronous file copy process
	 */
	private class CopyProcess implements Callable<Path> {
		
		private JobID jobID;
		private String filePath;
		private Boolean executable;

		public CopyProcess(DistributedCacheEntry e, JobID jobID) {
			this.filePath = e.filePath;
			this.executable = e.isExecutable;
			this.jobID = jobID;
		}
		@Override
		public Path call()  {
			Path tmp = getTempDir(jobID, filePath.substring(filePath.lastIndexOf("/") + 1));
			try {
				synchronized (lock) {
					copy(new Path(filePath), tmp, this.executable);
				}
			} catch (IOException e) {
				LOG.error("Could not copy file to local file cache.", e);
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
		private String filePath;

		public DeleteProcess(String name, DistributedCacheEntry e, JobID jobID) {
			this.name = name;
			this.jobID = jobID;
			this.filePath = e.filePath;
		}
		@Override
		public void run() {
			Path tmp = getTempDir(jobID, filePath.substring(filePath.lastIndexOf("/") + 1));
			try {
				synchronized (lock) {
					Map<String, Integer> count = jobCounts.get(jobID);
					if (count.containsKey(name)) {
						count.put(name, count.get(name) - 1);
						if (count.get(name) == 0) {
							if (lfs.exists(tmp)) {
								lfs.delete(tmp, true);
							}
							count.remove(name);
							if (count.isEmpty()) { //delete job directory
								tmp = getTempDir(jobID, "");
								if (lfs.exists(tmp)) {
									lfs.delete(tmp, true);
								}
								jobCounts.remove(jobID);
							}
						}
					}
				}
			} catch (IOException e) {
				LOG.error("Could not delete file from local file cache.", e);
			}
		}
	}
}
