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

	private Map<Pair<JobID, String>, Boolean> active = new HashMap<Pair<JobID,String>, Boolean>();

	private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10, ExecutorThreadFactory.INSTANCE);

	/**
	 * If the file doesn't exists locally, it will copy the file to the temp directory.
	 */
	public FutureTask<Path> createTmpFile(String name, String filePath, JobID jobID) {

		synchronized (active) {
			active.put(new ImmutablePair(jobID,name), true);
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
		synchronized (active) {
			active.put(new ImmutablePair(jobID, name), false);
		}
		DeleteProcess dp = new DeleteProcess(name, jobID);
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
				e.printStackTrace();
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
					byte [] buffer = new byte[DistributedCache.DEFAULT_BUFFER_SIZE];
					int num = fsInput.read(buffer);
					while (num != -1) {
						lfsOutput.write(buffer, 0, num);
						num = fsInput.read(buffer);
					}
					fsInput.close();
					lfsOutput.close();
				}
			} catch (IOException e1) {
				e1.printStackTrace();
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

		public DeleteProcess(String name, JobID jobID) {
			this.name = name;
			this.jobID = jobID;
		}

		public void run() {
			synchronized (active) {
				if (active.get(new ImmutablePair(jobID, name))) {
					return;
				}
			}
			Path tmp = getTempDir(jobID, name);
			try {
				if (lfs.exists(tmp)) {
					lfs.delete(tmp, true);
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
}
