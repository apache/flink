/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.util.SegmentsUtil;

/**
 * Binary format that in {@link MemorySegment}s.
 */
public abstract class BinaryFormat {

	protected MemorySegment[] segments;
	protected int offset;
	protected int sizeInBytes;

	public BinaryFormat() {}

	public BinaryFormat(MemorySegment[] segments, int offset, int sizeInBytes) {
		this.segments = segments;
		this.offset = offset;
		this.sizeInBytes = sizeInBytes;
	}

	public final void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
		pointTo(new MemorySegment[] {segment}, offset, sizeInBytes);
	}

	public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
		this.segments = segments;
		this.offset = offset;
		this.sizeInBytes = sizeInBytes;
	}

	public MemorySegment[] getSegments() {
		return segments;
	}

	public int getOffset() {
		return offset;
	}

	public int getSizeInBytes() {
		return sizeInBytes;
	}

	@Override
	public boolean equals(Object o) {
		return this == o || o != null &&
				getClass() == o.getClass() &&
				binaryEquals((BinaryFormat) o);
	}

	protected boolean binaryEquals(BinaryFormat that) {
		return sizeInBytes == that.sizeInBytes &&
				SegmentsUtil.equals(segments, offset, that.segments, that.offset, sizeInBytes);
	}

	@Override
	public int hashCode() {
		return SegmentsUtil.hash(segments, offset, sizeInBytes);
	}
}
