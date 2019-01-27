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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * To specify offset range of a file.
 */
public class FileOffsetRange implements Serializable {

	private static final long serialVersionUID = 7069346824354308L;

	private long start;

	private long size;

	public FileOffsetRange(long start, long size) {
		this.start = Preconditions.checkNotNull(start);
		this.size = Preconditions.checkNotNull(size);
	}

	public FileOffsetRange normalize(long fileLength) {
		long count = this.size > fileLength ? fileLength : this.size;
		long startTmp = this.start < 0 ? (fileLength + this.start) : this.start;
		if (startTmp <= 0) {
			startTmp = 0L;
		} else if (startTmp >= fileLength){
			startTmp = fileLength - count;
		} else {
			startTmp = startTmp;
		}
		long sizeTmp = Math.abs(this.size > fileLength || this.size == FileOffsetRange.getSizeDefaultValue() ? fileLength : this.size);
		long end = sizeTmp + startTmp;
		if (end > fileLength) {
			sizeTmp =  fileLength - startTmp;
		} else {
			sizeTmp =  sizeTmp;
		}
		this.start = startTmp;
		this.size = sizeTmp;
		return this;
	}

	public long getStart() {
		return this.start;
	}

	public long getSize() {
		return this.size;
	}

	public static long getStartDefaultValue(){
		return 0L;
	}

	public static long getSizeDefaultValue(){
		return -1L;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == FileOffsetRange.class) {
			FileOffsetRange that = (FileOffsetRange) obj;
			return this.getStart() == that.getStart() && this.getSize() == that.getSize();
		} else {
			return false;
		}
	}

	@Override
	public final int hashCode() {
		return Objects.hash(this.start, this.size);
	}

	@Override
	public String toString() {
		return "start=" + start + ", size=" + size;
	}
}
