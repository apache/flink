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

import java.io.File;
import java.io.Serializable;
import java.util.Objects;

/**
 * FileOffsetRange is used to decide which part of files to read.
 */
public class FileOffsetRange implements Serializable{
	public static final FileOffsetRange MAX_FILE_OFFSET_RANGE = new FileOffsetRange(0, Long.MAX_VALUE);
	private final long start;
	private final long end;

	public FileOffsetRange(long start, long end) {
		this.start = start;
		this.end = end;
	}

	public long getLengthForFile(File file) {
		return Math.min(end, file.length()) - Math.min(start, file.length());
	}

	public long getStartOffsetForFile(File file) {
		return Math.min(start, file.length());
	}

	public long getEndOffsetForFile(File file) {
		return Math.min(end, file.length());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (null == o || this.getClass() != o.getClass()) {
			return false;
		}

		FileOffsetRange that = (FileOffsetRange) o;
		return Objects.equals(start, that.start) &&
			Objects.equals(end, that.end);
	}

	@Override
	public int hashCode() {
		return Objects.hash(start, end);
	}

	public String toString() {
		return "Range={start=" + start + ", end=" + end + "}";
	}
}
