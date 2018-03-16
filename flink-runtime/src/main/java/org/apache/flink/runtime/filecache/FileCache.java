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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The FileCache is used to create the local files for the registered cache files when a task is deployed.
 * The files will be removed when the task is unregistered after a 5 second delay.
 * A given file x will be placed in "{@code <system-tmp-dir>/tmp_<jobID>/}".
 */
public class FileCache {

	static final Logger LOG = LoggerFactory.getLogger(FileCache.class);

	/** cache-wide lock to ensure consistency. copies are not done under this lock. */
	private final Object lock = new Object();

	private final Map<JobID, Map<String, Tuple4<Integer, File, Path, Future<Path>>>> entries;

	private final ScheduledExecutorService executorService;

	private final File[] storageDirectories;

	private final Thread shutdownHook;

	private int nextDirectory;

	// ------------------------------------------------------------------------

	public FileCache(String[] tempDirectories) throws IOException {

		Preconditions.checkNotNull(tempDirectories);

		storageDirectories = new File[tempDirectories.length];

		for (int i = 0; i < tempDirectories.length; i++) {
			String cacheDirName = "flink-dist-cache-" + UUID.randomUUID().toString();
			storageDirectories[i] = new File(tempDirectories[i], cacheDirName);
			String path = storageDirectories[i].getAbsolutePath();

			if (storageDirectories[i].mkdirs()) {
				LOG.info("User file cache uses directory " + path);
			} else {
				LOG.error("User file cache cannot create directory " + path);
				// delete all other directories we created so far
				for (int k = 0; k < i; k++) {
					if (!storageDirectories[k].delete()) {
						LOG.warn("User file cache cannot remove prior directory " +
								storageDirectories[k].getAbsolutePath());
					}
				}
				throw new IOException("File cache cannot create temp storage directory: " + path);
			}
		}

		this.shutdownHook = createShutdownHook(this, LOG);

		this.entries = new HashMap<JobID, Map<String, Tuple4<Integer, File, Path, Future<Path>>>>();
		this.executorService = Executors.newScheduledThreadPool(10,
				new ExecutorThreadFactory("flink-file-cache"));
	}

	/**
	 * Shuts down the file cache by cancelling all.
	 */
	public void shutdown() {
		synchronized (lock) {
			// first shutdown the thread pool
			ScheduledExecutorService es = this.executorService;
			if (es != null) {
				es.shutdown();
				try {
					es.awaitTermination(5000L, TimeUnit.MILLISECONDS);
				}
				catch (InterruptedException e) {
					// may happen
				}
			}

			entries.clear();

			// clean up the all storage directories
			for (File dir : storageDirectories) {
				try {
					FileUtils.deleteDirectory(dir);
				}
				catch (IOException e) {
					LOG.error("File cache could not properly clean up storage directory.");
				}
			}

			// Remove shutdown hook to prevent resource leaks
			ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * If the file doesn't exists locally, it will copy the file to the temp directory.
	 *
	 * @param name  The name under which the file is registered.
	 * @param entry The cache entry descriptor (path, executable flag)
	 * @param jobID The ID of the job for which the file is copied.
	 * @return The handle to the task that copies the file.
	 */
	public Future<Path> createTmpFile(String name, DistributedCacheEntry entry, JobID jobID) {
		synchronized (lock) {
			Map<String, Tuple4<Integer, File, Path, Future<Path>>> jobEntries = entries.get(jobID);
			if (jobEntries == null) {
				jobEntries = new HashMap<String, Tuple4<Integer, File, Path, Future<Path>>>();
				entries.put(jobID, jobEntries);
			}

			// tuple is (ref-count, parent-temp-dir, cached-file-path, copy-process)
			Tuple4<Integer, File, Path, Future<Path>> fileEntry = jobEntries.get(name);
			if (fileEntry != null) {
				// file is already in the cache. return a future that
				// immediately returns the file
				fileEntry.f0 = fileEntry.f0 + 1;

				// return the future. may be that the copy is still in progress
				return fileEntry.f3;
			}
			else {
				// need to copy the file

				// create the target path
				File tempDirToUse = new File(storageDirectories[nextDirectory++], jobID.toString());
				if (nextDirectory >= storageDirectories.length) {
					nextDirectory = 0;
				}

				String sourceFile = entry.filePath;
				int posOfSep = sourceFile.lastIndexOf("/");
				if (posOfSep > 0) {
					sourceFile = sourceFile.substring(posOfSep + 1);
				}

				Path target = new Path(tempDirToUse.getAbsolutePath() + "/" + sourceFile);

				// kick off the copying
				CopyProcess cp = new CopyProcess(entry, target);
				FutureTask<Path> copyTask = new FutureTask<Path>(cp);
				executorService.submit(copyTask);

				// store our entry
				jobEntries.put(name, new Tuple4<Integer, File, Path, Future<Path>>(1, tempDirToUse, target, copyTask));

				return copyTask;
			}
		}
	}

	/**
	 * Deletes the local file after a 5 second delay.
	 *
	 * @param name  The name under which the file is registered.
	 * @param jobID The ID of the job for which the file is copied.
	 */
	public void deleteTmpFile(String name, JobID jobID) {
		DeleteProcess dp = new DeleteProcess(lock, entries, name, jobID);
		executorService.schedule(dp, 5000L, TimeUnit.MILLISECONDS);
	}

	boolean holdsStillReference(String name, JobID jobId) {
		Map<String, Tuple4<Integer, File, Path, Future<Path>>> jobEntries = entries.get(jobId);
		if (jobEntries != null) {
			Tuple4<Integer, File, Path, Future<Path>> entry = jobEntries.get(name);
			return entry != null && entry.f0 > 0;
		}
		else {
			return false;
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	public static void copy(Path sourcePath, Path targetPath, boolean executable) throws IOException {
		// TODO rewrite this to make it participate in the closable registry and the lifecycle of a task.
		// we unwrap the file system to get raw streams without safety net
		FileSystem sFS = FileSystem.getUnguardedFileSystem(sourcePath.toUri());
		FileSystem tFS = FileSystem.getUnguardedFileSystem(targetPath.toUri());
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
				try (FSDataOutputStream lfsOutput = tFS.create(targetPath, FileSystem.WriteMode.NO_OVERWRITE); FSDataInputStream fsInput = sFS.open(sourcePath)) {
					IOUtils.copyBytes(fsInput, lfsOutput);
					//noinspection ResultOfMethodCallIgnored
					new File(targetPath.toString()).setExecutable(executable);
				} catch (IOException ioe) {
					LOG.error("could not copy file to local file cache.", ioe);
				}
			}
		}
	}

	private static Thread createShutdownHook(final FileCache cache, final Logger logger) {

		return ShutdownHookUtil.addShutdownHook(
			cache::shutdown,
			FileCache.class.getSimpleName(),
			logger
		);
	}

	// ------------------------------------------------------------------------
	//  background processes
	// ------------------------------------------------------------------------

	/**
	 * Asynchronous file copy process.
	 */
	private static class CopyProcess implements Callable<Path> {

		private final Path filePath;
		private final Path cachedPath;
		private boolean executable;

		public CopyProcess(DistributedCacheEntry e, Path cachedPath) {
			this.filePath = new Path(e.filePath);
			this.executable = e.isExecutable;
			this.cachedPath = cachedPath;
		}

		@Override
		public Path call() throws IOException {
			// let exceptions propagate. we can retrieve them later from
			// the future and report them upon access to the result
			copy(filePath, cachedPath, this.executable);
			return cachedPath;
		}
	}

	/**
	 * If no task is using this file after 5 seconds, clear it.
	 */
	private static class DeleteProcess implements Runnable {

		private final Object lock;
		private final Map<JobID, Map<String, Tuple4<Integer, File, Path, Future<Path>>>> entries;

		private final String name;
		private final JobID jobID;

		public DeleteProcess(Object lock, Map<JobID, Map<String, Tuple4<Integer, File, Path, Future<Path>>>> entries,
								String name, JobID jobID) {
			this.lock = lock;
			this.entries = entries;
			this.name = name;
			this.jobID = jobID;
		}

		@Override
		public void run() {
			try {
				synchronized (lock) {
					Map<String, Tuple4<Integer, File, Path, Future<Path>>> jobEntries = entries.get(jobID);

					if (jobEntries != null) {
						Tuple4<Integer, File, Path, Future<Path>> entry = jobEntries.get(name);

						if (entry != null) {
							int count = entry.f0;
							if (count > 1) {
								// multiple references still
								entry.f0 = count - 1;
							}
							else {
								// we remove the last reference
								jobEntries.remove(name);
								if (jobEntries.isEmpty()) {
									entries.remove(jobID);
								}

								// abort the copy
								entry.f3.cancel(true);

								// remove the file
								File file = new File(entry.f2.toString());
								if (file.exists()) {
									if (file.isDirectory()) {
										FileUtils.deleteDirectory(file);
									}
									else if (!file.delete()) {
										LOG.error("Could not delete locally cached file " + file.getAbsolutePath());
									}
								}

								// remove the job wide temp directory, if it is now empty
								File parent = entry.f1;
								if (parent.isDirectory()) {
									String[] children = parent.list();
									if (children == null || children.length == 0) {
										//noinspection ResultOfMethodCallIgnored
										parent.delete();
									}
								}
							}
						}
					}
				}
			}
			catch (IOException e) {
				LOG.error("Could not delete file from local file cache.", e);
			}
		}
	}
}
