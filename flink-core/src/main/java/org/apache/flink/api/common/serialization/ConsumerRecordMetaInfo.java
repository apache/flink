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

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.Public;

/**
 * The consumer record meta info contains, besides the actual message, some meta information, such as
 * key, topic, partition, offset and timestamp for Apache kafka
 *
 * <p><b>Note:</b>The timestamp is only valid for Kafka clients 0.10+, for older versions the value has the value `Long.MinValue` and
 * the timestampType has the value `NO_TIMESTAMP_TYPE`.
 */
@Public
public interface ConsumerRecordMetaInfo {
	/**
	 * The TimestampType is introduced in the kafka clients 0.10+. This interface is also used for the Kafka connector 0.9
	 * so a local enumeration is needed.
	 */
	enum TimestampType {
		NO_TIMESTAMP_TYPE, CREATE_TIME, INGEST_TIME
	}

	/**
	 * @return the key as a byte array (null if no key has been set).
	 */
	byte[] getKey();

	/**
	 * @return The message, as a byte array (null if the message was empty or deleted).
	 */
	byte[] getMessage();

	/**
	 * @return The topic the message has originated from (for example the Kafka topic).
	 */
	String getTopic();

	/**
	 * @return The partition the message has originated from (for example the Kafka partition).
	 */
	int getPartition();

	/**
	 * @return the offset of the message in the original source (for example the Kafka offset).
	 */
	long getOffset();

	/**
	 * @return the timestamp of the consumer record
	 */
	long getTimestamp();

	/**
	 * @return The timestamp type, could be NO_TIMESTAMP_TYPE, CREATE_TIME or INGEST_TIME.
	 */
	TimestampType getTimestampType();
}
