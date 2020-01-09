/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.filesystem.streaming;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.filesystem.PartitionComputer;
import org.apache.flink.table.filesystem.PartitionPathUtils;

/**
 */
public class TableBucketAssigner<T> implements BucketAssigner<T, String> {

	private final PartitionComputer<T> computer;

	public TableBucketAssigner(PartitionComputer<T> computer) {
		this.computer = computer;
	}

	@Override
	public String getBucketId(T element, Context context) {
		try {
			return PartitionPathUtils.generatePartitionPath(
					computer.generatePartValues(element));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public SimpleVersionedSerializer<String> getSerializer() {
		return SimpleVersionedStringSerializer.INSTANCE;
	}
}
