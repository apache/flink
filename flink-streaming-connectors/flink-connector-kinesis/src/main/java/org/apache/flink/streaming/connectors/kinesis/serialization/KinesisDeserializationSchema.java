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

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.io.IOException;
import java.io.Serializable;

/**
 * This is a deserialization schema specific for the Flink Kinesis Consumer. Different from the
 * basic {@link DeserializationSchema}, this schema offers additional Kinesis-specific information
 * about the record that may be useful to the user application.
 *
 * @param <T> The type created by the keyed deserialization schema.
 */
public interface KinesisDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

	/**
	 * Deserializes a Kinesis record's bytes
	 *
	 * @param recordKey the records's key as a byte array (null if no key has been set for the record)
	 * @param recordValue the record's value as a byte array
	 * @param stream the name of the Kinesis stream that this record was sent to
	 * @param seqNum the sequence number of this record in the Kinesis shard
	 * @return the deserialized message as an Java object
	 * @throws IOException
	 */
	T deserialize(byte[] recordKey, byte[] recordValue, String stream, String seqNum) throws IOException;

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * @param nextElement the element to test for the end-of-stream signal
	 * @return true if the element signals end of stream, false otherwise
	 */
	boolean isEndOfStream(T nextElement);
}
