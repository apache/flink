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


import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.io.File;
import java.util.Set;
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
		} catch (Exception  e) {
			throw new RuntimeException("Error while getting file from distributed cache", e);
		}
		return new File(tmp.toString());
	}
}
