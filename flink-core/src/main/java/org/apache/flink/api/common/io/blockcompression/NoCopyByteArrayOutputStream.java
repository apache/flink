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

package org.apache.flink.api.common.io.blockcompression;

import java.io.ByteArrayOutputStream;

/**
 * Warning: This.buf will be changed if the output data size exceeds original buffer length and it won't
 * generate any exception since ByteArrayOutputStream just grows its buffer automatically. So check written
 * bytes is necessary to avoid incomplete data.
 */
class NoCopyByteArrayOutputStream extends ByteArrayOutputStream {
	private int off;

	NoCopyByteArrayOutputStream(byte[] buf, int off) {
		super(0);
		reuse(buf, off);
	}

	void reuse(byte[] buf, int off) {
		this.buf = buf;
		this.count = off;
		this.off = off;
	}

	@Override
	public void reset() {
		this.count = this.off;
	}

	int getNumBytesWritten() {
		return this.count - this.off;
	}
}
