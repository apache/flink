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

package org.apache.flink.streaming.util.serialization;

import java.io.Serializable;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public interface DeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

	/**
	 * Deserializes the incoming data.
	 * 
	 * @param message
	 *            The incoming message in a byte array
	 * @return The deserialized message in the required format.
	 */
	T deserialize(byte[] message);

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted
	 * 
	 * @param nextElement
	 *            The element to test for end signal
	 * @return The end signal, if true the stream shuts down
	 */
	boolean isEndOfStream(T nextElement);
}
