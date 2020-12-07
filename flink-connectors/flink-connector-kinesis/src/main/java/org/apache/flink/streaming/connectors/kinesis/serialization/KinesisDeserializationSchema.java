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

package org.apache.flink.streaming.connectors.kinesis.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.IOException;
import java.io.Serializable;

/**
 * This is a deserialization schema specific for the Flink Kinesis Consumer. Different from the
 * basic {@link DeserializationSchema}, this schema offers additional Kinesis-specific information
 * about the record that may be useful to the user application.
 *
 * @param <T> The type created by the keyed deserialization schema.
 */
@PublicEvolving
public interface KinesisDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

	/**
	 * Initialization method for the schema. It is called before the actual working methods
	 * {@link #deserialize} and thus suitable for one time setup work.
	 *
	 * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access additional features such as e.g.
	 * registering user metrics.
	 *
	 * @param context Contextual information that can be used during initialization.
	 */
	default void open(DeserializationSchema.InitializationContext context) throws Exception {
	}

	/**
	 * Deserializes a Kinesis record's bytes. If the record cannot be deserialized, {@code null}
	 * may be returned. This informs the Flink Kinesis Consumer to process the Kinesis record
	 * without producing any output for it, i.e. effectively "skipping" the record.
	 *
	 * @param recordValue the record's value as a byte array
	 * @param partitionKey the record's partition key at the time of writing
	 * @param seqNum the sequence number of this record in the Kinesis shard
	 * @param approxArrivalTimestamp the server-side timestamp of when Kinesis received and stored the record
	 * @param stream the name of the Kinesis stream that this record was sent to
	 * @param shardId The identifier of the shard the record was sent to
	 *
	 * @return the deserialized message as an Java object ({@code null} if the message cannot be deserialized).
	 * @throws IOException
	 */
	T deserialize(byte[] recordValue, String partitionKey, String seqNum, long approxArrivalTimestamp, String stream, String shardId) throws IOException;

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * @param nextElement the element to test for the end-of-stream signal
	 * @return true if the element signals end of stream, false otherwise
	 */
	// TODO FLINK-4194 ADD SUPPORT FOR boolean isEndOfStream(T nextElement);
}
