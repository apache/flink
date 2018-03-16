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

package org.apache.flink.queryablestate.network.messages;

import org.apache.flink.annotation.Internal;

/**
 * The base class for every message exchanged during the communication between
 * {@link org.apache.flink.queryablestate.network.Client client} and
 * {@link org.apache.flink.queryablestate.network.AbstractServerBase server}.
 *
 * <p>Every such message should also have a {@link MessageDeserializer}.
 */
@Internal
public abstract class MessageBody {

	/**
	 * Serializes the message into a byte array.
	 * @return A byte array with the serialized content of the message.
	 */
	public abstract byte[] serialize();
}
