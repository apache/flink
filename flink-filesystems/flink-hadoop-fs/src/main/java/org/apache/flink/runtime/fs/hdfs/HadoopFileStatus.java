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
 * Hadoop Distributed File System.
 */
public class HadoopFileStatus implements FileStatus {

	private final org.apache.hadoop.fs.FileStatus fileStatus;

	/**
	 * Creates a new file status from an HDFS file status.
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
		return Math.min(fileStatus.getBlockSize(), fileStatus.getLen());
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

	@Override
	public Path getPath() {
		return new Path(fileStatus.getPath().toUri());
	}

	@Override
	public boolean isDir() {
		return fileStatus.isDirectory();
	}

	public org.apache.hadoop.fs.FileStatus getInternalFileStatus() {
		return this.fileStatus;
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@code HadoopFileStatus} from Hadoop's {@link org.apache.hadoop.fs.FileStatus}.
	 * If Hadoop's file status is <i>located</i>, i.e., it contains block information, then this method
	 * returns an implementation of Flink's {@link org.apache.flink.core.fs.LocatedFileStatus}.
	 */
	public static HadoopFileStatus fromHadoopStatus(final org.apache.hadoop.fs.FileStatus fileStatus) {
		return fileStatus instanceof org.apache.hadoop.fs.LocatedFileStatus
				? new LocatedHadoopFileStatus((org.apache.hadoop.fs.LocatedFileStatus) fileStatus)
				: new HadoopFileStatus(fileStatus);
	}
}
