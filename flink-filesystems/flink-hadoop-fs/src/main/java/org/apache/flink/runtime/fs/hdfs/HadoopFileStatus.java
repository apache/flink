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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

/**
 * Concrete implementation of the {@link FileStatus} interface for the
 * Hadoop Distribution File System.
 */
public final class HadoopFileStatus implements FileStatus {

	private org.apache.hadoop.fs.FileStatus fileStatus;

	/**
	 * Creates a new file status from a HDFS file status.
	 *
	 * @param fileStatus
	 *        the HDFS file status
	 */
	public HadoopFileStatus(org.apache.hadoop.fs.FileStatus fileStatus) {
		this.fileStatus = fileStatus;
	}

	@Override
	public long getLen() {
		return fileStatus.getLen();
	}

	@Override
	public long getBlockSize() {
		long blocksize = fileStatus.getBlockSize();
		if (blocksize > fileStatus.getLen()) {
			return fileStatus.getLen();
		}

		return blocksize;
	}

	@Override
	public long getAccessTime() {
		return fileStatus.getAccessTime();
	}

	@Override
	public long getModificationTime() {
		return fileStatus.getModificationTime();
	}

	@Override
	public short getReplication() {
		return fileStatus.getReplication();
	}

	public org.apache.hadoop.fs.FileStatus getInternalFileStatus() {
		return this.fileStatus;
	}

	@Override
	public Path getPath() {
		return new Path(fileStatus.getPath().toUri());
	}

	@SuppressWarnings("deprecation")
	@Override
	public boolean isDir() {
		return fileStatus.isDir();
	}
}
