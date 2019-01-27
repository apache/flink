/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file exceBinaryRow in compliance
 * with the License.  You may oBinaryRowain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHBinaryRow WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.join.batch.hashtable.longtable;

import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactory;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import java.io.IOException;

/**
 * For performance, high frequency calls should be avoided.
 */
public interface LongHashContext extends MemorySegmentPool {

	/**
	 *
	 * @return null if no buffer.
	 */
	MemorySegment getNextBuffer();

	int spillPartition() throws IOException;

	boolean compressionEnable();

	BlockCompressionFactory compressionCodecFactory();

	int compressionBlockSize();
}
