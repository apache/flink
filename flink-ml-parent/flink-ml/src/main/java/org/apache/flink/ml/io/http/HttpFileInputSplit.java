/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.io.http;

import org.apache.flink.core.io.InputSplit;

/**
 * Http File Input Split.
 */
public class HttpFileInputSplit implements InputSplit {
	public int numSplits;
	public int splitNo;
	public boolean splitable;
	public long contentLength = -1;
	public String etag = null;
	public String path = null;

	public long start = 0;
	public long length = -1;
	public long end = 0;

	private static final long BUFFER_SIZE = 1024L * 1024L;

	@Override
	public String toString() {
		return "split: " + splitNo + "/" + numSplits + ", " + start + " " + length + " " + end;
	}

	public HttpFileInputSplit(
		String path, int numSplits, int splitNo, boolean splitable, long contentLength, String etag) {
		this.path = path;
		this.numSplits = numSplits;
		this.splitNo = splitNo;
		this.splitable = splitable;
		this.contentLength = contentLength;
		this.etag = etag;

		long avg = contentLength / numSplits;
		long remain = contentLength % numSplits;
		this.length = avg + (splitNo < remain ? 1 : 0);
		this.start = splitNo * avg + Long.min(remain, splitNo);
		this.end = Long.min(start + length + BUFFER_SIZE, contentLength);
	}

	@Override
	public int getSplitNumber() {
		return this.splitNo;
	}
}
