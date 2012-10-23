/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.fs.hdfs;

import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.Path;

/**
 * Concrete implementation of the {@link FileStatus} interface for the
 * Hadoop Distribution File System.
 * 
 * @author warneke
 */
public final class DistributedFileStatus implements FileStatus {

	private org.apache.hadoop.fs.FileStatus fileStatus;

	/**
	 * Creates a new file status from a HDFS file status.
	 * 
	 * @param fileStatus
	 *        the HDFS file status
	 */
	public DistributedFileStatus(org.apache.hadoop.fs.FileStatus fileStatus) {
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

		return new Path(fileStatus.getPath().toString());
	}

	@Override
	public boolean isDir() {

		return fileStatus.isDir();
	}
}
