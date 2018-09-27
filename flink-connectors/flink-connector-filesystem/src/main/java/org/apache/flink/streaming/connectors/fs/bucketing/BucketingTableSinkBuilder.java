/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.flink.types.Row;

/**
 * A builder to configure and build the BucketingTableSink.
 */
public class BucketingTableSinkBuilder {

	private final BucketingSink<Row> sink;

	/**
	 * Specify the basePath of BucketingTableSink.
	 *
	 * @param basePath the basePath of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder(String basePath) {
		this.sink = new BucketingSink<Row>(basePath);
	}

	/**
	 * Specify the batchSize of BucketingTableSink.
	 *
	 * @param batchSize the batchSize of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setBatchSize(long batchSize) {
		this.sink.setBatchSize(batchSize);
		return this;
	}

	/**
	 * Specify the batchRolloverInterval of BucketingTableSink.
	 *
	 * @param batchRolloverInterval the batchRolloverInterval of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setBatchRolloverInterval(long batchRolloverInterval) {
		this.sink.setBatchRolloverInterval(batchRolloverInterval);
		return this;
	}

	/**
	 * Specify the bucketCheckInterval of BucketingTableSink.
	 *
	 * @param interval the bucketCheckInterval of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setInactiveBucketCheckInterval(long interval) {
		this.sink.setInactiveBucketCheckInterval(interval);
		return this;
	}

	/**
	 * Specify the inactiveBucketThreshold of BucketingTableSink.
	 *
	 * @param threshold the inactiveBucketThreshold of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setInactiveBucketThreshold(long threshold) {
		this.sink.setInactiveBucketThreshold(threshold);
		return this;
	}

	/**
	 * Specify the bucketer of BucketingTableSink.
	 *
	 * @param bucketer the bucketer of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setBucketer(Bucketer<Row> bucketer) {
		this.sink.setBucketer(bucketer);
		return this;
	}

	/**
	 * Specify the writer of BucketingTableSink.
	 *
	 * @param writer the writer of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setWriter(Writer<Row> writer) {
		this.sink.setWriter(writer);
		return this;
	}

	/**
	 * Specify the inProgressSuffix of BucketingTableSink.
	 *
	 * @param inProgressSuffix the inProgressSuffix of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setInProgressSuffix(String inProgressSuffix) {
		this.sink.setInProgressSuffix(inProgressSuffix);
		return this;
	}

	/**
	 * Specify the inProgressPrefix of BucketingTableSink.
	 *
	 * @param inProgressPrefix the inProgressPrefix of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setInProgressPrefix(String inProgressPrefix) {
		this.sink.setInProgressPrefix(inProgressPrefix);
		return this;
	}

	/**
	 * Specify the pendingSuffix of BucketingTableSink.
	 *
	 * @param pendingSuffix the pendingSuffix of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setPendingSuffix(String pendingSuffix) {
		this.sink.setPendingSuffix(pendingSuffix);
		return this;
	}

	/**
	 * Specify the pendingPrefix of BucketingTableSink.
	 *
	 * @param pendingPrefix the pendingPrefix of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setPendingPrefix(String pendingPrefix) {
		this.sink.setPendingPrefix(pendingPrefix);
		return this;
	}

	/**
	 * Specify the validLengthSuffix of BucketingTableSink.
	 *
	 * @param validLengthSuffix the validLengthSuffix of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setValidLengthSuffix(String validLengthSuffix) {
		this.sink.setValidLengthSuffix(validLengthSuffix);
		return this;
	}

	/**
	 * Specify the validLengthPrefix of BucketingTableSink.
	 *
	 * @param validLengthPrefix the validLengthPrefix of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setValidLengthPrefix(String validLengthPrefix) {
		this.sink.setValidLengthPrefix(validLengthPrefix);
		return this;
	}

	/**
	 * Specify the partSuffix of BucketingTableSink.
	 *
	 * @param partSuffix the partSuffix of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setPartSuffix(String partSuffix) {
		this.sink.setPartSuffix(partSuffix);
		return this;
	}

	/**
	 * Specify the partPrefix of BucketingTableSink.
	 *
	 * @param partPrefix the partPrefix of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setPartPrefix(String partPrefix) {
		this.sink.setPartPrefix(partPrefix);
		return this;
	}

	/**
	 * Specify the useTruncate of BucketingTableSink.
	 *
	 * @param useTruncate the useTruncate of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setUseTruncate(boolean useTruncate) {
		this.sink.setUseTruncate(useTruncate);
		return this;
	}

	/**
	 * Specify the asyncTimeout of BucketingTableSink.
	 *
	 * @param timeout the asyncTimeout of the BucketingTableSink.
	 */
	public BucketingTableSinkBuilder setAsyncTimeout(long timeout) {
		this.sink.setAsyncTimeout(timeout);
		return this;
	}

	/**
	 * Finalizes the configuration and checks validity.
	 *
	 * @return BucketingTableSink
	 */
	public BucketingTableSink build() {
		return new BucketingTableSink(sink);
	}

}
