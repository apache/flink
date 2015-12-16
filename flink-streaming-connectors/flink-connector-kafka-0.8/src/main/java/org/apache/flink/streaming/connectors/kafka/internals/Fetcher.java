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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;
import java.util.HashMap;

/**
 * A fetcher pulls data from Kafka, from a fix set of partitions.
 * The fetcher supports "seeking" inside the partitions, i.e., moving to a different offset.
 */
public interface Fetcher {

	/**
	 * Closes the fetcher. This will stop any operation in the
	 * {@link #run(SourceFunction.SourceContext, KeyedDeserializationSchema, HashMap)} method and eventually
	 * close underlying connections and release all resources.
	 */
	void close() throws IOException;

	/**
	 * Starts fetch data from Kafka and emitting it into the stream.
	 * 
	 * <p>To provide exactly once guarantees, the fetcher needs emit a record and update the update
	 * of the last consumed offset in one atomic operation:</p>
	 * <pre>{@code
	 * 
	 * while (running) {
	 *     T next = ...
	 *     long offset = ...
	 *     int partition = ...
	 *     synchronized (sourceContext.getCheckpointLock()) {
	 *         sourceContext.collect(next);
	 *         lastOffsets[partition] = offset;
	 *     }
	 * }
	 * }</pre>
	 *
	 * @param <T> The type of elements produced by the fetcher and emitted to the source context.
	 * @param sourceContext The source context to emit elements to.
	 * @param valueDeserializer The deserializer to decode the raw values with.
	 * @param lastOffsets The map into which to store the offsets for which elements are emitted (operator state)
	 */
	<T> void run(SourceFunction.SourceContext<T> sourceContext, KeyedDeserializationSchema<T> valueDeserializer,
				HashMap<KafkaTopicPartition, Long> lastOffsets) throws Exception;
	
	/**
	 * Set the next offset to read from for the given partition.
	 * For example, if the partition <i>i</i> offset is set to <i>n</i>, the Fetcher's next result
	 * will be the message with <i>offset=n</i>.
	 * 
	 * @param topicPartition The partition for which to seek the offset.
	 * @param offsetToRead To offset to seek to.
	 */
	void seek(KafkaTopicPartition topicPartition, long offsetToRead);

	/**
	 * Exit run loop with given error and release all resources.
	 *
	 * @param t Error cause
	 */
	void stopWithError(Throwable t);
}
