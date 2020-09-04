/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.internals.publisher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;

/**
 * A {@code RecordPublisher} will consume records from an external stream and deliver them to the registered subscriber.
 */
@Internal
public interface RecordPublisher {

	/**
	 * Run the record publisher. Records will be consumed from the stream and published to the consumer.
	 * The number of batches retrieved by a single invocation will vary based on implementation.
	 *
	 * @param recordBatchConsumer the record batch consumer in which to output records
	 * @return a status enum to represent whether a shard has been fully consumed
	 * @throws InterruptedException
	 */
	RecordPublisherRunResult run(RecordBatchConsumer recordBatchConsumer) throws InterruptedException;

	/**
	 * A status enum to represent whether a shard has been fully consumed.
	 */
	enum RecordPublisherRunResult {
		/** There are no more records to consume from this shard. */
		COMPLETE,

		/** There are more records to consume from this shard. */
		INCOMPLETE
	}

	/**
	 * An interface used to collect record batches, and reply with the latest consumed sequence number.
	 */
	interface RecordBatchConsumer {

		SequenceNumber accept(RecordBatch recordBatch);
	}
}
