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

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.io.Serializable;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * To specify detail of read file.
 */
public class FileReadDetail implements Serializable {

	private static final long serialVersionUID = -7265172816138616979L;
	private final ResourceID taskManagerResourceId;
	private final String fileName;
	private final FileOffsetRange fileOffsetRange;

	public FileReadDetail(ResourceID taskManagerResourceId, String fileName, Long start, Long size){
		this.taskManagerResourceId = taskManagerResourceId;
		this.fileName = fileName;
		if (start == null || size == null){
			this.fileOffsetRange = null;
		} else {
			this.fileOffsetRange = new FileOffsetRange(start, size);
		}
	}

	public ResourceID getTaskManagerResourceId() {
		return taskManagerResourceId;
	}

	public String getFileName() {
		return fileName;
	}

	public FileOffsetRange getFileOffsetRange() {
		return fileOffsetRange;
	}

	@Override
	public int hashCode() {
		return Objects.hash(taskManagerResourceId, fileName, fileOffsetRange);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == FileReadDetail.class) {
			FileReadDetail that = (FileReadDetail) obj;
			return Objects.equals(taskManagerResourceId, that.getTaskManagerResourceId()) &&
				Objects.equals(fileName, that.getFileName()) &&
				Objects.equals(fileOffsetRange, that.getFileOffsetRange());
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		String fileOffsetRangeStr = this.fileOffsetRange == null ? "" : this.fileOffsetRange.toString();
		String fileName = this.fileName == null ? "" : this.fileName;
		StringJoiner joiner = new StringJoiner(",");
		joiner.add("taskManagerResourceId=" + taskManagerResourceId.toString());
		joiner.add("fileName=" + fileName);
		joiner.add("fileOffsetRange=(" + fileOffsetRangeStr + ")");
		return joiner.toString();
	}
}
