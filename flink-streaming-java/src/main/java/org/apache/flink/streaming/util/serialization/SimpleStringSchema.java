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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Very simple serialization schema for strings.
 */
@PublicEvolving
public class SimpleStringSchema implements DeserializationSchema<String>, SerializationSchema<String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String deserialize(byte[] message) {
		return new String(message);
	}

	@Override
	public boolean isEndOfStream(String nextElement) {
		return false;
	}

	@Override
	public byte[] serialize(String element) {
		return element.getBytes();
	}

	@Override
	public TypeInformation<String> getProducedType() {
		return BasicTypeInfo.STRING_TYPE_INFO;
	}
}
