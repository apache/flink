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

package org.apache.flink.table.format.single;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;

import java.io.IOException;

/**
 * single value serializer for string .
 */
public class StringSingleValueSerializer extends SingleValueSerializer<StringData> {

	@Override
	public StringData deserialize(byte[] message) throws IOException {
		return StringData.fromBytes(message);
	}

	@Override
	public boolean isEndOfStream(StringData nextElement) {
		return false;
	}

	@Override
	public byte[] serialize(StringData element) {
		return element.toBytes();
	}

	@Override
	public TypeInformation<StringData> getProducedType() {
		return StringDataTypeInfo.INSTANCE;
	}

}
