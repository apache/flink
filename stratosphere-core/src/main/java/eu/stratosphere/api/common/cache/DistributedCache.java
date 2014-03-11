package eu.stratosphere.api.common.cache;


import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.Path;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * DistributedCache provides static method to write the registered cache files into job configuration or decode
 * them from job configuration. It also provides user access to the file locally.
 */
public class DistributedCache {

	final static String CACHE_FILE_NUM = "DISTRIBUTED_CACHE_FILE_NUM";

	final static String CACHE_FILE_NAME = "DISTRIBUTED_CACHE_FILE_NAME_";

	final static String CACHE_FILE_PATH = "DISTRIBUTED_CACHE_FILE_PATH_";

	public final static String TMP_PREFIX = "tmp_";

	public final static int DEFAULT_BUFFER_SIZE = 8192;

	private Map<String, FutureTask<Path>> cacheCopyTasks = new HashMap<String, FutureTask<Path>>();

	public static void addCachedFile(String name, String filePath, Configuration conf) {
		int num = conf.getInteger(CACHE_FILE_NUM,0) + 1;
		conf.setInteger(CACHE_FILE_NUM, num);
		conf.setString(CACHE_FILE_NAME + num, name);
		conf.setString(CACHE_FILE_PATH + num, filePath);
	}

	public static Set<Entry<String,String>> getCachedFile(Configuration conf) {
		Map<String, String> cacheFiles = new HashMap<String, String>();
		int num = conf.getInteger(CACHE_FILE_NUM,0);
		for (int i = 1; i <= num; i++) {
			String name = conf.getString(CACHE_FILE_NAME + i, "");
			String filePath = conf.getString(CACHE_FILE_PATH + i, "");
			cacheFiles.put(name, filePath);
		}
		return cacheFiles.entrySet();
	}

	public void setCopyTasks(Map<String, FutureTask<Path>> cpTasks) {
			this.cacheCopyTasks = cpTasks;
	}

	public File getFile(String name) {
		Path tmp = null;
		//The FutureTask.get() method will block until the file is ready.
		try {
			tmp = cacheCopyTasks.get(name).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return new File(tmp.toString());
	}
}
