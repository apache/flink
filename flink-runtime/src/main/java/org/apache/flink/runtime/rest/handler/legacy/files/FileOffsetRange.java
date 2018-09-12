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

package org.apache.flink.runtime.rest.handler.legacy.files;

import java.io.File;
import java.io.Serializable;

/**
 * FileOffsetRange is used to decide which part of files to read.
 */
public class FileOffsetRange implements Serializable{
	private static final String SEPARATOR = "-";
	private final long start;
	private final long end;

	private FileOffsetRange(long start, long end) {
		this.start = start;
		this.end = end;
	}

	public static final FileOffsetRange MAX_FILE_OFFSET_RANGE = new FileOffsetRange(0, Long.MAX_VALUE);

	public static FileOffsetRange generateRange(String range) {
		if (range == null) {
			return MAX_FILE_OFFSET_RANGE;
		}
		String[] content = range.split(SEPARATOR);
		long start = 0;
		long end = Long.MAX_VALUE;
		if (content.length == 2) {
			start = parseStartOffset(content[0]);
			end = parseEndOffset(content[1]);
		} else if (content.length == 1) {
			start = parseStartOffset(content[0]);
		}
		if (start > end) {
			start = end;
		}
		return new FileOffsetRange(start, end);
	}

	private static long parseStartOffset(String start) {
		try {
			return Long.parseLong(start);
		} catch (Exception e) {
			return 0;
		}
	}

	private static long parseEndOffset(String end) {
		try {
			return Long.parseLong(end);
		} catch (Exception e) {
			return Long.MAX_VALUE;
		}
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

	public String toString() {
		return start + SEPARATOR + end;
	}
}
