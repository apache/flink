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

package org.apache.flink.api.common.messages;

import org.apache.flink.types.Value;

/**
 * An example implementation of a message which tasks can broadcast to parallel instances of themselves.
 */
public class KeyValueMessage implements TaskMessage {
	private String key;
	private Value value;

	public KeyValueMessage(String _key, Value _value){
		key = _key;
		value = _value;
	}

	public String getKey(){
		return key;
	}

	public Value getValue(){
		return value;
	}
}
