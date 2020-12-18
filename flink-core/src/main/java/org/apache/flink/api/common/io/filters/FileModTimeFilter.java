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

package org.apache.flink.api.common.io.filters;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.util.Preconditions;

/**
 * A filter that only keeps files whose their modification time is no less than a given timestamp
 * as consuming time position. It basically sets a read start position in file system for Flink.
 */
public class FileModTimeFilter implements FileFilter {
	/**
	 * The consuming time position to process files whose their modification time is no less than this timestamp.
	 */
	private long consumingTimePosition;

	public FileModTimeFilter(long consumingTimePosition) {
		Preconditions.checkArgument(consumingTimePosition >= 0);

		this.consumingTimePosition = consumingTimePosition;
	}

	@Override
	public boolean accept(FileStatus fileStatus) {
		// Always accept dir, because it may contain files newer than itself
		return fileStatus.isDir() || fileStatus.getModificationTime() >= consumingTimePosition;
	}
}
