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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class JavaDefaultStringSchema implements DeserializationSchema<String>, SerializationSchema<String, byte[]> {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean isEndOfStream(String nextElement) {
		return nextElement.equals("q");
	}

	@Override
	public byte[] serialize(String element) {
		return SerializationUtils.serialize(element);
	}

	@Override
	public String deserialize(byte[] message) {
		return SerializationUtils.deserialize(message);
	}

	@Override
	public TypeInformation<String> getProducedType() {
		return TypeExtractor.getForClass(String.class);
	}

}