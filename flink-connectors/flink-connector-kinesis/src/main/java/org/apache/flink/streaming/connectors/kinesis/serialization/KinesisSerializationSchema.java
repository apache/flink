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

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Kinesis-specific serialization schema, allowing users to specify a target stream based
 * on a record's contents.
 * @param <T>
 */
@PublicEvolving
public interface KinesisSerializationSchema<T> extends Serializable {
	/**
	 * Serialize the given element into a ByteBuffer.
	 *
	 * @param element The element to serialize
	 * @return Serialized representation of the element
	 */
	ByteBuffer serialize(T element);

	/**
	 * Optional method to determine the target stream based on the element.
	 * Return <code>null</code> to use the default stream
	 *
	 * @param element The element to determine the target stream from
	 * @return target stream name
	 */
	String getTargetStream(T element);
}
