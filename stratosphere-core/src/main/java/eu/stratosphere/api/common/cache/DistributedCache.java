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

package eu.stratosphere.api.common.cache;


import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.FutureTask;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;

/**
 * DistributedCache provides static methods to write the registered cache files into job configuration or decode
 * them from job configuration. It also provides user access to the file locally.
 */
public class DistributedCache {
	
	public static class DistributedCacheEntry {
		public String filePath;
		public Boolean isExecutable;
		
		public DistributedCacheEntry(String filePath, Boolean isExecutable){
			this.filePath=filePath;
			this.isExecutable=isExecutable;
		}
	}

	final static String CACHE_FILE_NUM = "DISTRIBUTED_CACHE_FILE_NUM";

	final static String CACHE_FILE_NAME = "DISTRIBUTED_CACHE_FILE_NAME_";

	final static String CACHE_FILE_PATH = "DISTRIBUTED_CACHE_FILE_PATH_";
	
	final static String CACHE_FILE_EXE = "DISTRIBUTED_CACHE_FILE_EXE_";

	public final static String TMP_PREFIX = "tmp_";

	private Map<String, FutureTask<Path>> cacheCopyTasks = new HashMap<String, FutureTask<Path>>();

	public static void writeFileInfoToConfig(String name, DistributedCacheEntry e, Configuration conf) {
		int num = conf.getInteger(CACHE_FILE_NUM,0) + 1;
		conf.setInteger(CACHE_FILE_NUM, num);
		conf.setString(CACHE_FILE_NAME + num, name);
		conf.setString(CACHE_FILE_PATH + num, e.filePath);
		conf.setBoolean(CACHE_FILE_EXE + num, e.isExecutable || new File(e.filePath).canExecute());
	}

	public static Set<Entry<String, DistributedCacheEntry>> readFileInfoFromConfig(Configuration conf) {
		Map<String, DistributedCacheEntry> cacheFiles = new HashMap<String, DistributedCacheEntry>();
		int num = conf.getInteger(CACHE_FILE_NUM, 0);
		for (int i = 1; i <= num; i++) {
			String name = conf.getString(CACHE_FILE_NAME + i, "");
			String filePath = conf.getString(CACHE_FILE_PATH + i, "");
			Boolean isExecutable = conf.getBoolean(CACHE_FILE_EXE + i, false);
			cacheFiles.put(name, new DistributedCacheEntry(filePath, isExecutable));
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
		} catch (Exception  e) {
			throw new RuntimeException("Error while getting file from distributed cache", e);
		}
		return new File(tmp.toString());
	}
}
