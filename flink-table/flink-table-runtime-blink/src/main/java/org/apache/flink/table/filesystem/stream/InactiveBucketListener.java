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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.streaming.api.functions.sink.filesystem.Bucket;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketLifeCycleListener;
import org.apache.flink.table.data.RowData;

import java.util.function.Consumer;

/**
 * Inactive {@link BucketLifeCycleListener} to obtain inactive buckets to consumer.
 */
public class InactiveBucketListener implements BucketLifeCycleListener<RowData, String> {

	private transient Consumer<String> inactiveConsumer;

	public void setInactiveConsumer(Consumer<String> inactiveConsumer) {
		this.inactiveConsumer = inactiveConsumer;
	}

	@Override
	public void bucketCreated(Bucket<RowData, String> bucket) {
	}

	@Override
	public void bucketInactive(Bucket<RowData, String> bucket) {
		inactiveConsumer.accept(bucket.getBucketId());
	}
}
