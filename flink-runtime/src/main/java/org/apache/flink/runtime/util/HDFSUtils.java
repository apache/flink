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

package org.apache.flink.runtime.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.CHECKPOINT_DIR_PREFIX;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.METADATA_FILE_NAME;

public class HDFSUtils {
	private static final Logger log = LoggerFactory.getLogger(HDFSUtils.class);

	public static String HDFS_PREFIX = "hdfs";

	public static String VIEWFS_PREFIX = "viewfs";

	public static String getFullPathForLatestJobCompletedCheckpointMeta(String path) throws Exception {
		// get job directory list
		FileStatus[] jobList = getFileListByHDFSPath(path);

		if(jobList != null && jobList.length >= 2) {
			// get the latest job by modified time
			String latestJobDir = getLatestJobDirectory(jobList);
			log.info("Latest job directory: {}", latestJobDir);

			if(latestJobDir != null && !latestJobDir.isEmpty()) {
				FileStatus[] jobSubList = getFileListByHDFSPath(path + "/" + latestJobDir);
				if(jobSubList != null && jobSubList.length > 1) {
					for(FileStatus fileStatus2: jobSubList) {
						log.info("Sub directory {} for latest job {}", fileStatus2.getPath().getName(), latestJobDir);
						if(fileStatus2.getPath().getName().contains(CHECKPOINT_DIR_PREFIX)) {
							return path + "/" + latestJobDir + "/" + fileStatus2.getPath().getName() + "/" + METADATA_FILE_NAME;
						}
					}
				}
			}
		}

		return null;
	}

	private static String getLatestJobDirectory(FileStatus[] jobList) {
		String result = null;
		// the max modify time directory is for the current job
		long maxModifyTime = 0L;
		// the latest job directory
		long secondModifyTime = 0L;
		for (FileStatus fileStatus: jobList) {
			if(fileStatus.getModificationTime() > maxModifyTime) {
				maxModifyTime = fileStatus.getModificationTime();
			}
		}

		log.info("Max modify time: {}", maxModifyTime);

		for (FileStatus fileStatus: jobList) {
			if(fileStatus.getModificationTime() != maxModifyTime && fileStatus.getModificationTime() > secondModifyTime) {
				secondModifyTime = fileStatus.getModificationTime();
				result = fileStatus.getPath().getName();
				log.info("Set second directory to {} with time {}", result, secondModifyTime);
			}
		}
		return result;
	}

	public static FileStatus[] getFileListByHDFSPath(String path) throws Exception {
		if (path == null || path.isEmpty()) {
			throw new Exception("HDFS path is null");
		}

		FileStatus[] status = null;
		log.info("Get file list by hdfs path: {}", path);

		try {
			FileSystem fs = FileSystem.get(new Configuration());
			status = fs.listStatus(new Path(path));
			log.info("File List for path {}", path);
			for (FileStatus fileStatus: status) {
				log.info("name: {}, modify time: {}", fileStatus.getPath().getName(), fileStatus.getModificationTime());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return status;
	}
}
